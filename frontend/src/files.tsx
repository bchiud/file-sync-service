import CloudUploadIcon from "@mui/icons-material/CloudUpload";
import DeleteIcon from "@mui/icons-material/Delete";
import DownloadIcon from "@mui/icons-material/Download";
import FolderOpenOutlinedIcon from "@mui/icons-material/FolderOpenOutlined";
import ShareIcon from "@mui/icons-material/Share";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  LinearProgress,
  Paper,
  Skeleton,
  Stack,
  Tooltip,
  Typography
} from "@mui/material";
import { useEffect, useState } from "react";
import { connectChanges } from "./lib/changes";
import {
  ApiError,
  commit,
  deleteFile,
  downloadUrls,
  type FileSummary,
  listFiles,
  missingBlocks,
  recipe,
  uploadUrls
} from "./lib/api";
import { chunkFile } from "./lib/crypto";
import { assembleBlocks, saveBlob } from "./lib/download";
import { displayPath, downloadName, formatSize, middleTruncate } from "./lib/format";
import { SharePanel } from "./shares";

export function FileList() {
  // --- state ---
  const [loading, setLoading] = useState(true);
  const [files, setFiles] = useState<FileSummary[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false); // global: Upload button + progress bar
  const [busyKey, setBusyKey] = useState<string | null>(null); // the one row in flight
  const [openPath, setOpenPath] = useState<string | null>(null);

  // --- data loading ---
  async function refresh() {
    try {
      setFiles(await listFiles());
      setLoading(false);
    } catch {
      setError("Could not load files");
    }
  }

  useEffect(() => {
    refresh(); // initial load on mount
    return connectChanges(() => refresh()); // + live updates after
  }, []);

  // --- upload ---
  async function upload(file: File) {
    // the server rejects "\" in paths (422); check before chunking/uploading any blocks
    if (file.name.includes("\\")) {
      setError(`Can't upload "${file.name}": filenames can't contain "\\".`);
      return;
    }
    setUploading(true);
    setError(null);
    try {
      const blocks = await chunkFile(file); // [{hash, blob}, ...]
      const allHashes = blocks.map((b) => b.hash); // full recipe, in order
      const missing = await missingBlocks(allHashes); // what the server lacks
      const toSend = blocks.filter((b) => missing.includes(b.hash));

      const urls = toSend.length ? await uploadUrls(missing) : {};
      await Promise.all(
        toSend.map(async (b) => {
          const res = await fetch(urls[b.hash], {
            method: "PUT",
            body: b.blob,
          });
          if (!res.ok) throw new Error(`block PUT ${b.hash} → ${res.status}`);
        }),
      );

      const existing = files.find((f) => f.path === "/" + file.name);
      try {
        await commit(file.name, file.size, allHashes, existing?.etag); // record the recipe; etag => update, undefined => create
      } catch (e) {
        if (e instanceof ApiError && e.status === 412) {
          setError(
            "This file changed in another session — reload and try again.",
          );
          await refresh(); // pull the new etag so the retry can succeed
          return;
        }
        throw e;
      }

      await refresh(); // reload the list
    } catch {
      setError("Upload failed");
    } finally {
      setUploading(false);
    }
  }

  // --- download ---
  async function download(f: FileSummary) {
    setBusyKey(f.path);
    setError(null);
    try {
      const rec = await recipe(f.path, f.owner);
      const urls = await downloadUrls(rec.block_hashes, f.owner, f.path);
      const blob = await assembleBlocks(rec.block_hashes, urls);
      saveBlob(blob, downloadName(f.path));
    } catch (e) {
      console.error(e);
      setError(e instanceof Error ? e.message : "Download failed");
    } finally {
      setBusyKey(null);
    }
  }

  // --- remove ---
  async function remove(f: FileSummary) {
    if (!window.confirm(`Delete ${displayPath(f.path)}?`)) return;
    setBusyKey(f.path);
    setError(null);
    try {
      await deleteFile(f.path);
      await refresh();
    } catch {
      setError("Delete failed");
    } finally {
      setBusyKey(null);
    }
  }

  // --- render ---
  function toggleShare(path: string) {
    setOpenPath(openPath === path ? null : path);
  }

  return (
    <>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Button
        variant="contained"
        component="label"
        disabled={uploading}
        startIcon={<CloudUploadIcon />}
        sx={{ mb: 1 }}
      >
        Upload file
        <input
          type="file"
          hidden
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) upload(file);
          }}
        />
      </Button>
      {uploading && <LinearProgress sx={{ mb: 2 }} />}

      {!loading && !error && files.length === 0 ? (
        <Paper
          variant="outlined"
          sx={{ p: 6, textAlign: "center", borderRadius: 2 }}
        >
          <FolderOpenOutlinedIcon
            sx={{ fontSize: 44, color: "text.disabled" }}
          />
          <Typography sx={{ mt: 1.5, fontWeight: 500 }}>
            No files yet
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Upload a file and it appears here.
          </Typography>
        </Paper>
      ) : (
        <Stack spacing={1}>
          {loading
            ? [0, 1, 2].map((i) => (
                <Skeleton key={i} variant="rounded" height={56} />
              ))
            : files.map((f) => {
                const rowBusy = busyKey === f.path;
                return (
                  <Paper key={f.path} variant="outlined" sx={{ p: 1 }}>
                    <Stack
                      direction="row"
                      spacing={1}
                      sx={{ alignItems: "center" }}
                    >
                      {/* minWidth:0 lets flex item shrink below its content;
                      ellipsize (middle truncate) text to fit within column */}
                      <Box sx={{ flexGrow: 1, minWidth: 0 }}>
                        <Tooltip title={displayPath(f.path)} enterDelay={400}>
                          <Typography noWrap>
                            {middleTruncate(displayPath(f.path))}
                          </Typography>
                        </Tooltip>
                        <Typography variant="body2" color="text.secondary">
                          {formatSize(f.size)}
                        </Typography>
                      </Box>
                      <Stack direction="row" spacing={1} sx={{ flexShrink: 0 }}>
                        <Button
                          size="small"
                          startIcon={
                            rowBusy ? (
                              <CircularProgress size={14} color="inherit" />
                            ) : (
                              <DownloadIcon />
                            )
                          }
                          onClick={() => download(f)}
                          disabled={rowBusy}
                        >
                          Download
                        </Button>
                        <Button
                          size="small"
                          startIcon={<ShareIcon />}
                          onClick={() => toggleShare(f.path)}
                        >
                          Share
                        </Button>
                        <Button
                          size="small"
                          startIcon={<DeleteIcon />}
                          onClick={() => remove(f)}
                          disabled={rowBusy}
                          sx={{ color: "error.light" }}
                        >
                          Delete
                        </Button>
                      </Stack>
                    </Stack>
                    {openPath === f.path && <SharePanel path={f.path} />}
                  </Paper>
                );
              })}
        </Stack>
      )}
    </>
  );
}
