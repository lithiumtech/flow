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

package com.lithium.flow.config.repos;

import static com.google.common.base.Preconditions.checkNotNull;

import com.lithium.flow.config.Config;
import com.lithium.flow.config.Repo;
import com.lithium.flow.config.exception.ConfigNotFoundException;
import com.lithium.flow.util.Logs;
import com.lithium.flow.util.Measure;
import com.lithium.flow.util.Needle;
import com.lithium.flow.util.Progress;
import com.lithium.flow.util.Threader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.slf4j.Logger;

/**
 * @author Matt Ayres
 */
public class ParallelRepo extends DecoratedRepo {
	private static final Logger log = Logs.getLogger();

	private final Threader threader;
	private final int retries;
	private final long logTime;
	private final long avgTime;

	public ParallelRepo(@Nonnull Repo delegate, @Nonnull Config config) {
		super(checkNotNull(delegate));
		checkNotNull(config);

		threader = Threader.build(config);
		// Threader retries a failed task as a new future that the needle never sees, so a read that
		// recovers on retry would still be lost. Retry inline instead so the result reaches the needle.
		retries = threader.getRetries();
		threader.setRetries(0);

		boolean progress = config.getBoolean("progress", false);
		logTime = progress ? config.getTime("progress.logTime", "5s") : 0;
		avgTime = progress ? config.getTime("progress.avgTime", "15s") : 0;
	}

	@Override
	@Nonnull
	public List<Config> getConfigs() throws IOException {
		List<String> nameList = getNames();
		Needle<Optional<Config>> needle = threader.needle();
		Progress progress = new Progress();
		Measure names = progress.measure("names").useForEta();

		if (logTime > 0) {
			progress.start(logTime, avgTime);
		}

		List<Optional<Config>> results;
		try {
			nameList.forEach(name -> {
				names.incTodo();

				needle.submit(name, () -> {
					try {
						return readConfig(name);
					} finally {
						names.incDone();
					}
				});
			});

			results = needle.toList();
		} finally {
			if (logTime > 0) {
				progress.close();
			}
		}

		List<Config> configs = new ArrayList<>();
		List<String> failed = new ArrayList<>();
		for (int i = 0; i < results.size(); i++) {
			Optional<Config> result = results.get(i);
			if (result == null) {
				failed.add(nameList.get(i));
			} else {
				result.ifPresent(configs::add);
			}
		}

		if (!failed.isEmpty()) {
			throw new IOException("failed to read " + failed.size() + " of " + nameList.size() + " configs: " + failed);
		}
		return configs;
	}

	@Nonnull
	private Optional<Config> readConfig(@Nonnull String name) throws IOException {
		for (int retriesLeft = retries; ; retriesLeft--) {
			try {
				return Optional.of(getConfig(name));
			} catch (ConfigNotFoundException e) {
				log.warn("skipping config that no longer exists: {} ({})", name, e.getMessage());
				return Optional.empty();
			} catch (IOException | RuntimeException e) {
				if (retriesLeft <= 0) {
					throw e;
				}
				log.warn("failed to read config: {} ({} retries left)", name, retriesLeft, e);
			}
		}
	}

	@Override
	@Nonnull
	public Stream<Config> streamConfigs() throws IOException {
		return getConfigs().stream();
	}
}
