/*
 * Copyright 2026 Lithium Technologies, Inc.
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

package com.lithium.flow.config.repos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.lithium.flow.config.Config;
import com.lithium.flow.config.Configs;
import com.lithium.flow.config.Repo;
import com.lithium.flow.config.exception.ConfigNotFoundException;
import com.lithium.flow.filer.CachedFiler;
import com.lithium.flow.filer.CachedReadFiler;
import com.lithium.flow.filer.Filer;
import com.lithium.flow.svn.SvnFiler;
import com.lithium.flow.svn.SvnProvider;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tmatesoft.svn.core.SVNDepth;
import org.tmatesoft.svn.core.SVNErrorCode;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNURL;
import org.tmatesoft.svn.core.internal.io.fs.FSRepositoryFactory;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.io.SVNRepositoryFactory;
import org.tmatesoft.svn.core.wc.SVNClientManager;

/**
 * Drives the production config chain ({@code Repos.buildRepo} / {@code Repos.buildFiler}) over a real local SVN
 * repository: SvnFiler -> CachedFiler(dirOnly) -> CachedReadFiler -> FilerRepo -> ParallelRepo -> ScheduledRepo.
 */
public class ParallelRepoTest {
	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	private SVNURL url;
	private SVNClientManager client;
	private SvnProvider provider;

	@Before
	public void setUp() throws Exception {
		FSRepositoryFactory.setup();
		url = SVNRepositoryFactory.createLocalRepository(temp.newFolder("svn"), true, false);
		client = SVNClientManager.newInstance();
		provider = new SvnProvider() {
			@Override
			@Nonnull
			public SVNURL getLocation() {
				return url;
			}

			@Override
			@Nonnull
			public SVNRepository getRepository() throws IOException {
				try {
					return SVNRepositoryFactory.create(url);
				} catch (SVNException e) {
					throw new IOException(e);
				}
			}

			@Override
			public void releaseRepository(@Nonnull SVNRepository repository) {
				repository.closeSession();
			}

			@Override
			public void close() {
			}
		};

		File src = temp.newFolder("src");
		File prod = new File(src, "prod");
		assertTrue(prod.mkdirs());
		write(prod, "c1.config", "key = one\n");
		write(prod, "c2.config", "!include prod/inc.include\nkey = two\n");
		write(prod, "c3.config", "key = three\n");
		write(prod, "inc.include", "included = yes\n");
		client.getCommitClient().doImport(src, url, "import", null, false, false, SVNDepth.INFINITY);
	}

	@After
	public void tearDown() {
		client.dispose();
	}

	@Test
	public void svnFilerReportsDeletedFileAsFileNotFound() throws Exception {
		SvnFiler filer = new SvnFiler(provider);
		delete("prod/c3.config");
		try {
			filer.readFile("prod/c3.config");
			fail("expected FileNotFoundException");
		} catch (FileNotFoundException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("prod/c3.config"));
		}
	}

	@Test
	public void readOfConfigDeletedAfterDirOnlyListingIsConfigNotFound() throws Exception {
		Repo repo = filerRepo();
		assertEquals(3, repo.getNames().size());
		delete("prod/c3.config");
		try {
			repo.getConfig("c3");
			fail("expected ConfigNotFoundException");
		} catch (IOException e) {
			assertTrue(e.toString(), e instanceof ConfigNotFoundException);
			SVNException svn = (SVNException) e.getCause().getCause();
			assertEquals(SVNErrorCode.FS_NOT_FOUND, svn.getErrorMessage().getErrorCode());
		}
	}

	@Test
	public void configDeletedAfterListingIsSkipped() throws Exception {
		Repo repo = new ParallelRepo(deleteAfterListing(filerRepo(), "prod/c3.config"), parallelConfig(3));
		assertEquals("[c1:one, c2:two]", describe(repo.getConfigs()));
	}

	@Test
	public void configDeletedAfterListingDoesNotBlockFirstScheduledPass() throws Exception {
		Repo parallel = new ParallelRepo(deleteAfterListing(filerRepo(), "prod/c3.config"), parallelConfig(3));
		try (Repo repo = new ScheduledRepo(parallel, TimeUnit.MINUTES.toMillis(5), 0)) {
			List<String> names = CompletableFuture.supplyAsync(() -> {
				try {
					return repo.getNames();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}).get(30, TimeUnit.SECONDS);
			Collections.sort(names);
			assertEquals("[c1, c2]", names.toString());
		}
	}

	@Test
	public void configWithoutEntryRecordIsSkipped() throws Exception {
		Filer filer = new SvnFiler(provider);
		Repo repo = new ParallelRepo(deleteAfterListing(new FilerRepo(Collections.singletonList(filer),
				Collections.singletonList("prod")), "prod/c3.config"), parallelConfig(0));
		assertEquals("[c1:one, c2:two]", describe(repo.getConfigs()));
	}

	@Test
	public void missingIncludeOfExistingConfigFailsThePass() throws Exception {
		Repo repo = new ParallelRepo(deleteAfterListing(filerRepo(), "prod/inc.include"), parallelConfig(1));
		try {
			repo.getConfigs();
			fail("expected IOException");
		} catch (IOException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("[c2]"));
		}
	}

	@Test
	public void readThatRecoversOnRetryIsKept() throws Exception {
		AtomicInteger attempts = new AtomicInteger();
		Repo flaky = new DecoratedRepo(filerRepo()) {
			@Override
			@Nonnull
			public Config getConfig(@Nonnull String name) throws IOException {
				if (name.equals("c1") && attempts.incrementAndGet() == 1) {
					throw new IOException("transient");
				}
				return super.getConfig(name);
			}
		};
		Repo repo = new ParallelRepo(flaky, parallelConfig(3));
		assertEquals("[c1:one, c2:two, c3:three]", describe(repo.getConfigs()));
		assertEquals(2, attempts.get());
	}

	@Test
	public void readThatKeepsFailingFailsThePassWithItsName() throws Exception {
		Repo broken = new DecoratedRepo(filerRepo()) {
			@Override
			@Nonnull
			public Config getConfig(@Nonnull String name) throws IOException {
				if (name.equals("c1")) {
					throw new IOException("broken");
				}
				return super.getConfig(name);
			}
		};
		Repo repo = new ParallelRepo(broken, parallelConfig(2));
		try {
			repo.getConfigs();
			fail("expected IOException");
		} catch (IOException e) {
			assertTrue(e.getMessage(), e.getMessage().contains("[c1]"));
		}
	}

	@Nonnull
	private Repo filerRepo() throws IOException {
		Config cacheConfig = Configs.newBuilder()
				.setString("cache.dirOnly", "true")
				.setString("cache.expireTime", "5m")
				.build();
		Filer filer = new SvnFiler(provider);
		filer = new CachedFiler(filer, cacheConfig);
		filer = new CachedReadFiler(filer);
		return new FilerRepo(Collections.singletonList(filer), Collections.singletonList("prod"));
	}

	@Nonnull
	private Repo deleteAfterListing(@Nonnull Repo delegate, @Nonnull String path) {
		AtomicInteger listings = new AtomicInteger();
		return new DecoratedRepo(delegate) {
			@Override
			@Nonnull
			public List<String> getNames() throws IOException {
				List<String> names = super.getNames();
				if (listings.incrementAndGet() == 1) {
					delete(path);
				}
				return names;
			}
		};
	}

	@Nonnull
	private static Config parallelConfig(int retries) {
		return Configs.newBuilder()
				.setString("threads", "4")
				.setString("retries", String.valueOf(retries))
				.build();
	}

	@Nonnull
	private static String describe(@Nonnull List<Config> configs) {
		return configs.stream()
				.map(config -> config.getName() + ":" + config.getString("key"))
				.sorted()
				.collect(Collectors.toList())
				.toString();
	}

	private void delete(@Nonnull String path) throws IOException {
		try {
			client.getCommitClient().doDelete(new SVNURL[] {url.appendPath(path, false)}, "delete " + path);
		} catch (SVNException e) {
			throw new IOException(e);
		}
	}

	private static void write(@Nonnull File dir, @Nonnull String name, @Nonnull String text) throws IOException {
		Files.write(new File(dir, name).toPath(), text.getBytes(StandardCharsets.UTF_8));
	}
}
