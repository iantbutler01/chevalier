-- Stable identity shared by multiple namespace entries. Existing files receive
-- their prior entry id, preserving mixed-version data without rewriting packs.
ALTER TABLE chevalier_vfs_entries
    ADD COLUMN IF NOT EXISTS file_id TEXT;

UPDATE chevalier_vfs_entries
SET file_id = id
WHERE entry_kind = 'file'
  AND file_id IS NULL;

CREATE INDEX IF NOT EXISTS chevalier_vfs_entries_file_identity_idx
    ON chevalier_vfs_entries (scope_key, file_id)
    WHERE entry_kind = 'file';
