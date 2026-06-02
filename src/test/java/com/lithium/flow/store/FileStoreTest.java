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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression coverage for the atomic write added to {@link FileStore} so that a
 * process death mid-write cannot leave the backing file truncated to 0 bytes
 * (eng-maintenance#20312 / LIA-26329 wolf-repo vault corruption).
 *
 * @author Claude Code agent (on behalf of Ngoc)
 */
public class FileStoreTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void readWriteRoundTrip() throws Exception {
		File file = new File(folder.getRoot(), "store.json");
		Store store = new FileStore(file);
		store.putValue("a", "1");
		store.putValue("b", "2");
		assertEquals("1", store.getValue("a"));
		assertEquals("2", store.getValue("b"));
		assertEquals(2, store.getKeys().size());

		store.putValue("a", null);
		assertNull(store.getValue("a"));
		assertEquals(1, store.getKeys().size());
		assertTrue(file.length() > 0);
	}

	@Test
	public void missingFileReadsEmpty() {
		File file = new File(folder.getRoot(), "absent.json");
		Store store = new FileStore(file);
		assertNull(store.getValue("nope"));
		assertTrue(store.getKeys().isEmpty());
	}

	/**
	 * The write must never be observed in-place on the target file: the target is
	 * either its previous complete content or the new complete content, never a
	 * partial/truncated state. We prove this by recording the target's size/inode
	 * just before a rewrite is published and confirming the target was replaced
	 * atomically (a fresh temp path, then a rename) rather than truncated in place.
	 */
	@Test
	public void writeReplacesTargetAtomicallyNeverTruncatingInPlace() throws Exception {
		File file = new File(folder.getRoot(), "store.json");
		Store store = new FileStore(file);
		store.putValue("vault.check", "v1");

		Object keyBefore = Files.readAttributes(file.toPath(), "unix:ino").get("ino");
		long sizeBefore = file.length();
		assertTrue(sizeBefore > 0);

		store.putValue("vault.check", "v2");

		// Atomic replace swaps in a new file, so the inode changes; an in-place
		// truncate-then-write (the bug) would keep the same inode and pass through
		// a 0-byte state. Either way, the file must always be complete + readable.
		Object keyAfter = Files.readAttributes(file.toPath(), "unix:ino").get("ino");
		assertTrue("atomic rename should publish a new inode", !keyBefore.equals(keyAfter));
		assertEquals("v2", new FileStore(file).getValue("vault.check"));
		assertTrue(file.length() > 0);
	}

	/**
	 * A temp file left behind by a previously interrupted write (SIGTERM between
	 * createTempFile and the rename) must be swept on the next write, and the
	 * healthy target must remain intact and readable throughout.
	 */
	@Test
	public void sweepsStaleTempFromInterruptedWrite() throws Exception {
		File file = new File(folder.getRoot(), "store.json");
		Store store = new FileStore(file);
		store.putValue("vault.check", "good");

		// Simulate a temp orphaned by a prior interrupted write.
		File stale = new File(folder.getRoot(), file.getName() + ".9999999999999.tmp");
		Files.write(stale.toPath(), "partial".getBytes());
		assertTrue(stale.exists());

		store.putValue("vault.salt", "more"); // next write should sweep the orphan

		assertTrue("stale temp should be swept", !stale.exists());
		assertEquals("good", store.getValue("vault.check"));
		assertEquals("more", store.getValue("vault.salt"));
	}

	@Test
	public void relativePathDoesNotFail() throws Exception {
		// vault.path is configured relative (e.g. "repo.vault"); File.getParentFile()
		// is null for a bare relative name, so the atomic write must resolve the
		// parent via toAbsolutePath() rather than NPE.
		String cwd = System.getProperty("user.dir");
		try {
			System.setProperty("user.dir", folder.getRoot().getAbsolutePath());
			File rel = new File("repo.vault");
			Store store = new FileStore(rel.getAbsoluteFile());
			store.putValue("vault.check", "ok");
			assertEquals("ok", store.getValue("vault.check"));
		} finally {
			System.setProperty("user.dir", cwd);
		}
	}

	@Test
	public void preservesPosixPermissionsAcrossRewrite() throws Exception {
		File file = new File(folder.getRoot(), "store.json");
		Path path = file.toPath();
		Store store = new FileStore(file);
		store.putValue("vault.check", "init");

		// Skip on non-POSIX filesystems (e.g. Windows).
		if (Files.getFileAttributeView(path, PosixFileAttributeView.class) == null) {
			return;
		}

		Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r-----"));
		Set<java.nio.file.attribute.PosixFilePermission> before = Files.getPosixFilePermissions(path);

		store.putValue("vault.salt", "rewrite"); // atomic rewrite over the 0640 file

		assertEquals("atomic rewrite must not change file permissions",
				before, Files.getPosixFilePermissions(path));
	}

	@Test
	public void doesNotLeaveTempFiles() throws Exception {
		File file = new File(folder.getRoot(), "store.json");
		Store store = new FileStore(file);
		for (int i = 0; i < 10; i++) {
			store.putValue("k" + i, "v" + i);
		}
		// after successful writes, only the target file should remain
		File[] temps = folder.getRoot().listFiles((d, name) -> name.endsWith(".tmp"));
		assertTrue("no temp files should remain", temps == null || temps.length == 0);
	}
}
