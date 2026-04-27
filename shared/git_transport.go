package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gogit "github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

const (
	channelsDir      = "channels"
	syncInterval     = 200 * time.Millisecond
	writeBatchWindow = 30 * time.Millisecond
)

type writeRequest struct {
	writes []writeEntry
	done   chan error
}

type writeEntry struct {
	relPath string
	content []byte
}

// GitSmartHTTPClient implements Transport via git push/pull (Smart HTTP).
// Background syncer fetches every syncInterval.
// Write calls are batched into a single push within a writeBatchWindow.
// Read and ListChannels are local filesystem reads — zero network overhead.
//
// Locking:
//   - mu protects the worktree (read concurrent, write exclusive). Network
//     fetches do NOT hold mu so concurrent Read calls are not blocked while
//     a fetch is in flight (a fetch can take 100ms–several seconds on a
//     congested link).
//   - fetchMu serializes concurrent fetches between the syncer and writer
//     to avoid two simultaneous git fetches against the same repo.
type GitSmartHTTPClient struct {
	auth       *githttp.BasicAuth
	repoURL    string
	mainBranch string
	mu         sync.RWMutex
	fetchMu    sync.Mutex
	repo       *gogit.Repository
	workDir    string
	writeCh    chan *writeRequest
	cancel     context.CancelFunc
}

func NewGitSmartHTTPClient(token, repo string) (*GitSmartHTTPClient, error) {
	parts := strings.SplitN(repo, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("git transport: repo must be 'owner/repo' format, got %q", repo)
	}

	repoURL := "https://github.com/" + repo + ".git"
	auth := &githttp.BasicAuth{
		Username: "x-token-auth",
		Password: token,
	}

	workDir, err := os.MkdirTemp("", "gh-tunnel-git-*")
	if err != nil {
		return nil, fmt.Errorf("git transport: create workdir: %w", err)
	}

	r, err := gogit.PlainClone(workDir, false, &gogit.CloneOptions{
		URL:  repoURL,
		Auth: auth,
	})
	if err != nil {
		if !isEmptyRepoError(err) {
			_ = os.RemoveAll(workDir)
			return nil, fmt.Errorf("git transport: clone %s: %w", repo, err)
		}
		r, err = seedEmptyRepo(workDir, repoURL, auth)
		if err != nil {
			_ = os.RemoveAll(workDir)
			return nil, fmt.Errorf("git transport: seed empty repo %s: %w", repo, err)
		}
	}

	mainBranch := "main"
	if head, herr := r.Head(); herr == nil {
		mainBranch = head.Name().Short()
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &GitSmartHTTPClient{
		auth:       auth,
		repoURL:    repoURL,
		mainBranch: mainBranch,
		repo:       r,
		workDir:    workDir,
		writeCh:    make(chan *writeRequest, 256),
		cancel:     cancel,
	}
	go c.runSyncer(ctx)
	go c.runBatchWriter(ctx)
	return c, nil
}

func (c *GitSmartHTTPClient) Close() error {
	c.cancel()
	return os.RemoveAll(c.workDir)
}

// ── Transport interface ───────────────────────────────────────────────────────

// EnsureChannel creates channels/{id}/client.json and channels/{id}/server.json
// with placeholder content and pushes. If existingID is non-empty and the
// directory already exists locally, it is returned as-is.
func (c *GitSmartHTTPClient) EnsureChannel(ctx context.Context, existingID string) (string, error) {
	if existingID != "" {
		dir := filepath.Join(c.workDir, channelsDir, existingID)
		if _, err := os.Stat(dir); err == nil {
			return existingID, nil
		}
	}

	id, err := GenerateID()
	if err != nil {
		return "", fmt.Errorf("EnsureChannel generate ID: %w", err)
	}
	if existingID != "" {
		id = existingID
	}

	placeholder, _ := json.Marshal(&Batch{Seq: 0, Ts: 0, Frames: []Frame{}})

	c.mu.Lock()
	defer c.mu.Unlock()

	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.syncToRemote(ctx); err != nil {
			return "", fmt.Errorf("EnsureChannel sync: %w", err)
		}
		w, err := c.repo.Worktree()
		if err != nil {
			return "", err
		}
		for _, fname := range []string{ClientBatchFile, ServerBatchFile} {
			relPath := filepath.Join(channelsDir, id, fname)
			if err := c.writeFileBytes(relPath, placeholder); err != nil {
				return "", fmt.Errorf("EnsureChannel write %s: %w", fname, err)
			}
			if _, err := w.Add(relPath); err != nil {
				return "", fmt.Errorf("EnsureChannel stage %s: %w", fname, err)
			}
		}
		pushErr := c.commitAndPush(ctx, w, "new channel: "+id)
		if pushErr == nil {
			break
		}
		if !isGitNonFastForward(pushErr) || attempt == maxRetries-1 {
			return "", fmt.Errorf("EnsureChannel push: %w", pushErr)
		}
	}
	return id, nil
}

// DeleteChannel removes channels/{channelID}/ and pushes the deletion.
func (c *GitSmartHTTPClient) DeleteChannel(ctx context.Context, channelID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.syncToRemote(ctx); err != nil {
			return fmt.Errorf("DeleteChannel sync: %w", err)
		}
		w, err := c.repo.Worktree()
		if err != nil {
			return fmt.Errorf("DeleteChannel worktree: %w", err)
		}

		removed := false
		for _, fname := range []string{ClientBatchFile, ServerBatchFile} {
			relPath := filepath.Join(channelsDir, channelID, fname)
			if err := os.Remove(filepath.Join(c.workDir, relPath)); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("DeleteChannel remove %s: %w", fname, err)
			}
			if _, err := w.Remove(relPath); err != nil {
				return fmt.Errorf("DeleteChannel stage %s: %w", fname, err)
			}
			removed = true
		}
		_ = os.Remove(filepath.Join(c.workDir, channelsDir, channelID))

		if !removed {
			return nil
		}
		pushErr := c.commitAndPush(ctx, w, "cleanup channel: "+channelID)
		if pushErr == nil {
			return nil
		}
		if !isGitNonFastForward(pushErr) || attempt == maxRetries-1 {
			return pushErr
		}
	}
	return nil
}

// ListChannels returns all subdirectories of channels/ from the local working tree.
func (c *GitSmartHTTPClient) ListChannels(_ context.Context) ([]*ChannelInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	dir := filepath.Join(c.workDir, channelsDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("ListChannels readdir: %w", err)
	}

	var channels []*ChannelInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, _ := e.Info()
		var updatedAt time.Time
		if info != nil {
			updatedAt = info.ModTime()
		}
		channels = append(channels, &ChannelInfo{
			ID:          e.Name(),
			Description: ChannelDescPrefix,
			UpdatedAt:   updatedAt,
		})
	}
	return channels, nil
}

// Write queues a batch write; concurrent calls within writeBatchWindow share one push.
func (c *GitSmartHTTPClient) Write(ctx context.Context, channelID, filename string, batch *Batch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("git Write marshal: %w", err)
	}
	req := &writeRequest{
		writes: []writeEntry{{
			relPath: filepath.Join(channelsDir, channelID, filename),
			content: data,
		}},
		done: make(chan error, 1),
	}
	select {
	case c.writeCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Read reads channels/{channelID}/{filename} from the local working tree.
// Returns (nil, nil) if the file doesn't exist or contains an empty batch.
func (c *GitSmartHTTPClient) Read(_ context.Context, channelID, filename string) (*Batch, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	absPath := filepath.Join(c.workDir, channelsDir, channelID, filename)
	data, err := os.ReadFile(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("git Read %s/%s: %w", channelID, filename, err)
	}
	var batch Batch
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("git Read %s/%s parse: %w", channelID, filename, err)
	}
	if batch.Seq == 0 && len(batch.Frames) == 0 {
		return nil, nil
	}
	return &batch, nil
}

func (c *GitSmartHTTPClient) GetRateLimitInfo() RateLimitInfo {
	return RateLimitInfo{
		Remaining:   99999,
		Limit:       99999,
		ResetAt:     time.Now().Add(1 * time.Hour),
		LastUpdated: time.Now(),
	}
}

// ── background goroutines ────────────────────────────────────────────────────

func (c *GitSmartHTTPClient) runSyncer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(syncInterval):
		}
		// Network fetch happens OUTSIDE c.mu so concurrent Read calls on the
		// worktree are not blocked while a fetch is in flight. The reset is
		// quick and runs under c.mu briefly.
		if err := c.fetchAndApply(ctx); err != nil {
			slog.Debug("git transport: background sync failed", "error", err)
		}
	}
}

// fetchAndApply fetches from origin, then briefly takes c.mu to apply the
// fetched HEAD to the worktree. This is what runSyncer calls every tick.
func (c *GitSmartHTTPClient) fetchAndApply(ctx context.Context) error {
	c.fetchMu.Lock()
	fetchErr := c.repo.FetchContext(ctx, &gogit.FetchOptions{
		RemoteName: "origin",
		Auth:       c.auth,
	})
	c.fetchMu.Unlock()
	if fetchErr != nil && fetchErr != gogit.NoErrAlreadyUpToDate {
		return fmt.Errorf("git fetch: %w", fetchErr)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applyFetchedHead()
}

// applyFetchedHead resets the worktree to the remote HEAD. Caller must hold c.mu.
func (c *GitSmartHTTPClient) applyFetchedHead() error {
	remoteRef := plumbing.NewRemoteReferenceName("origin", c.mainBranch)
	ref, err := c.repo.Reference(remoteRef, true)
	if err != nil {
		return fmt.Errorf("git resolve remote HEAD (%s): %w", remoteRef, err)
	}
	w, err := c.repo.Worktree()
	if err != nil {
		return fmt.Errorf("git worktree: %w", err)
	}
	return w.Reset(&gogit.ResetOptions{Commit: ref.Hash(), Mode: gogit.HardReset})
}

func (c *GitSmartHTTPClient) runBatchWriter(ctx context.Context) {
	for {
		var first *writeRequest
		select {
		case <-ctx.Done():
			return
		case first = <-c.writeCh:
		}

		collected := map[string][]*writeRequest{first.writes[0].relPath: {first}}
		timer := time.NewTimer(writeBatchWindow)
	coalesce:
		for {
			select {
			case req := <-c.writeCh:
				for _, we := range req.writes {
					collected[we.relPath] = append(collected[we.relPath], req)
				}
			case <-timer.C:
				break coalesce
			}
		}
		timer.Stop()

		type finalWrite struct {
			relPath string
			content []byte
		}
		seen := make(map[string]bool, len(collected))
		var order []finalWrite
		for path, reqs := range collected {
			if seen[path] {
				continue
			}
			seen[path] = true
			last := reqs[len(reqs)-1]
			for _, we := range last.writes {
				if we.relPath == path {
					order = append(order, finalWrite{path, we.content})
					break
				}
			}
		}

		c.mu.Lock()
		var pushErr error
		const maxRetries = 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			if syncErr := c.syncToRemote(ctx); syncErr != nil {
				pushErr = syncErr
				break
			}
			w, wtErr := c.repo.Worktree()
			if wtErr != nil {
				pushErr = wtErr
				break
			}
			var stageErr error
			for _, fw := range order {
				if err := c.writeFileBytes(fw.relPath, fw.content); err != nil {
					stageErr = fmt.Errorf("batch write %s: %w", fw.relPath, err)
					break
				}
				if _, err := w.Add(fw.relPath); err != nil {
					stageErr = fmt.Errorf("batch stage %s: %w", fw.relPath, err)
					break
				}
			}
			if stageErr != nil {
				pushErr = stageErr
				break
			}
			pushErr = c.commitAndPush(ctx, w, "tunnel data")
			if pushErr == nil || !isGitNonFastForward(pushErr) || attempt == maxRetries-1 {
				break
			}
			pushErr = nil
		}
		c.mu.Unlock()

		for _, reqs := range collected {
			for _, req := range reqs {
				req.done <- pushErr
			}
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// syncToRemote is called by writers (EnsureChannel, DeleteChannel, runBatchWriter)
// while holding c.mu — the fetch is serialized via fetchMu so it doesn't
// race the syncer's fetch.
//
// Caller must hold c.mu (write lock).
func (c *GitSmartHTTPClient) syncToRemote(ctx context.Context) error {
	c.fetchMu.Lock()
	fetchErr := c.repo.FetchContext(ctx, &gogit.FetchOptions{
		RemoteName: "origin",
		Auth:       c.auth,
	})
	c.fetchMu.Unlock()
	if fetchErr != nil && fetchErr != gogit.NoErrAlreadyUpToDate {
		return fmt.Errorf("git fetch: %w", fetchErr)
	}
	return c.applyFetchedHead()
}

func (c *GitSmartHTTPClient) writeFileBytes(relPath string, content []byte) error {
	absPath := filepath.Join(c.workDir, relPath)
	if err := os.MkdirAll(filepath.Dir(absPath), 0o700); err != nil {
		return err
	}
	return os.WriteFile(absPath, content, 0o600)
}

func (c *GitSmartHTTPClient) commitAndPush(ctx context.Context, w *gogit.Worktree, msg string) error {
	_, err := w.Commit(msg, &gogit.CommitOptions{
		Author: &object.Signature{
			Name:  "gh-tunnel",
			Email: "tunnel@localhost",
			When:  time.Now(),
		},
		AllowEmptyCommits: false,
	})
	if err != nil {
		s := strings.ToLower(err.Error())
		if strings.Contains(s, "nothing to commit") || strings.Contains(s, "clean") {
			return nil
		}
		return fmt.Errorf("git commit: %w", err)
	}
	pushErr := c.repo.PushContext(ctx, &gogit.PushOptions{Auth: c.auth})
	if pushErr == gogit.NoErrAlreadyUpToDate {
		return nil
	}
	return pushErr
}

func isEmptyRepoError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "remote repository is empty") ||
		strings.Contains(s, "repository is empty")
}

func seedEmptyRepo(workDir, repoURL string, auth *githttp.BasicAuth) (*gogit.Repository, error) {
	if err := os.RemoveAll(workDir); err != nil {
		return nil, fmt.Errorf("cleanup partial clone: %w", err)
	}
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return nil, fmt.Errorf("recreate workdir: %w", err)
	}
	r, err := gogit.PlainInitWithOptions(workDir, &gogit.PlainInitOptions{
		InitOptions: gogit.InitOptions{
			DefaultBranch: plumbing.NewBranchReferenceName("main"),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("init: %w", err)
	}
	if _, err = r.CreateRemote(&gitconfig.RemoteConfig{
		Name: "origin",
		URLs: []string{repoURL},
	}); err != nil {
		return nil, fmt.Errorf("add remote: %w", err)
	}
	w, err := r.Worktree()
	if err != nil {
		return nil, fmt.Errorf("worktree: %w", err)
	}
	keepPath := filepath.Join(workDir, ".gitkeep")
	if err := os.WriteFile(keepPath, []byte(""), 0o600); err != nil {
		return nil, fmt.Errorf("write .gitkeep: %w", err)
	}
	if _, err := w.Add(".gitkeep"); err != nil {
		return nil, fmt.Errorf("stage .gitkeep: %w", err)
	}
	if _, err = w.Commit("init: tunnel transport repository", &gogit.CommitOptions{
		Author: &object.Signature{
			Name:  "gh-tunnel",
			Email: "tunnel@localhost",
			When:  time.Now(),
		},
	}); err != nil {
		return nil, fmt.Errorf("initial commit: %w", err)
	}
	if err := r.PushContext(context.Background(), &gogit.PushOptions{
		RemoteName: "origin",
		Auth:       auth,
	}); err != nil && err != gogit.NoErrAlreadyUpToDate {
		return nil, fmt.Errorf("initial push: %w", err)
	}
	return r, nil
}

func isGitNonFastForward(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "non-fast-forward") ||
		strings.Contains(s, "rejected") ||
		strings.Contains(s, "cannot lock ref")
}
