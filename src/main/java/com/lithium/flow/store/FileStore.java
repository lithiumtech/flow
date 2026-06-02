/*
 * Copyright 2015 Lithium Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lithium.flow.store;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Matt Ayres
 */
public class FileStore implements Store {
	private final File file;
	private final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);

	public FileStore(@Nonnull File file) {
		this.file = checkNotNull(file);
	}

	@Override
	public void putValue(@Nonnull String key, @Nullable String value) {
		Map<String, String> map = read();
		if (value != null) {
			map.put(key, value);
		} else {
			map.remove(key);
		}
		write(map);
	}

	@Override
	@Nullable
	public String getValue(@Nonnull String key) {
		return read().get(key);
	}

	@Override
	@Nonnull
	public Set<String> getKeys() {
		return read().keySet();
	}

	@SuppressWarnings("unchecked")
	private synchronized Map<String, String> read() {
		if (file.exists()) {
			try {
				return mapper.readValue(file, Map.class);
			} catch (IOException e) {
				throw new StoreException("failed to read " + file, e);
			}
		} else {
			return new HashMap<>();
		}
	}

	private static final String TEMP_SUFFIX = ".tmp";

	private synchronized void write(@Nonnull Map<String, String> map) {
		try {
			// Write atomically: serialize to a temp file in the same directory,
			// fsync it, then atomically rename over the target. This prevents a
			// process death (e.g. SIGTERM) mid-write from leaving the file
			// truncated to 0 bytes, which would make the next read() fail.
			Path target = file.toPath().toAbsolutePath();
			Path dir = target.getParent();
			if (dir != null) {
				Files.createDirectories(dir);
			}

			// Sweep any temp files this store leaked on a prior interrupted write
			// (a SIGTERM between createTempFile and the rename bypasses the finally
			// cleanup below). Best-effort: keeps the directory from accumulating.
			sweepStaleTemps(dir, target);

			byte[] bytes = mapper.writeValueAsBytes(map);

			Path temp = Files.createTempFile(dir, file.getName() + ".", TEMP_SUFFIX);
			try {
				copyPermissions(target, temp);

				// Write the bytes and fsync to disk before the rename, so the new
				// content is durable. force(true) flushes data + metadata.
				try (FileChannel channel = FileChannel.open(temp,
						StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
					channel.write(ByteBuffer.wrap(bytes));
					channel.force(true);
				}

				try {
					Files.move(temp, target,
							StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
				} catch (AtomicMoveNotSupportedException e) {
					// Filesystem cannot do an atomic rename (some network mounts):
					// fall back to a plain replace, which is still far better than
					// truncate-in-place because the new content is already complete.
					Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
				}
			} finally {
				// If the rename succeeded, temp is already gone; this only cleans
				// up a temp left behind by a failure between create and move.
				Files.deleteIfExists(temp);
			}
		} catch (IOException e) {
			throw new StoreException("failed to write " + file, e);
		}
	}

	/**
	 * Best-effort: give the temp file the same POSIX permissions as the existing
	 * target so the atomic swap does not change the file's mode (e.g. a 0640
	 * vault must not silently become 0600). No-op on filesystems without POSIX
	 * support (e.g. Windows) so this stays portable -- earlier unconditional
	 * POSIX handling here was removed in FLOW-36 for breaking on Windows.
	 */
	private void copyPermissions(@Nonnull Path target, @Nonnull Path temp) {
		try {
			PosixFileAttributeView srcView =
					Files.getFileAttributeView(target, PosixFileAttributeView.class);
			PosixFileAttributeView dstView =
					Files.getFileAttributeView(temp, PosixFileAttributeView.class);
			if (srcView != null && dstView != null && Files.exists(target)) {
				Set<PosixFilePermission> perms = srcView.readAttributes().permissions();
				dstView.setPermissions(perms);
			}
		} catch (IOException | UnsupportedOperationException e) {
			// Non-POSIX filesystem or unreadable attributes: leave temp's default
			// permissions. This matches stock behavior for a freshly created file.
		}
	}

	/**
	 * Delete temp files this store left in the directory from a prior write that
	 * was interrupted (e.g. SIGTERM) between createTempFile and the rename. Only
	 * matches our own "<name>.NNN.tmp" prefix so unrelated files are untouched.
	 */
	private void sweepStaleTemps(@Nullable Path dir, @Nonnull Path target) {
		if (dir == null) {
			return;
		}
		String prefix = target.getFileName() + ".";
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, prefix + "*" + TEMP_SUFFIX)) {
			for (Path stale : stream) {
				try {
					Files.deleteIfExists(stale);
				} catch (IOException ignored) {
					// leave it; not worth failing the write over a stale temp
				}
			}
		} catch (IOException ignored) {
			// directory not listable: skip the sweep, the write can still proceed
		}
	}
}
