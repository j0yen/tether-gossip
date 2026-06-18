# Rollback Plan — tether-gossip

## Steps

1. `cd ~/wintermute/tether-gossip`
2. `git log --oneline` — identify the commit to revert to
3. `git revert HEAD` — or `git reset --hard <prior-sha>` for a hard rollback
4. Uninstall binary: `rm -f ~/.local/bin/wm-tether-gossip`
5. Restart any daemon that depends on this binary.

## Notes

- All commits are revert-clean (no merge commits, linear history).
- State file is at `~/.cache/wm-tether-gossip/state` — safe to delete to reset seq to 0.
