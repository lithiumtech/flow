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

package com.lithium.flow.filer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.lithium.flow.config.Config;
import com.lithium.flow.util.Caches;
import com.lithium.flow.util.CheckedFunction;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.cache.LoadingCache;

/**
 * Decorates an instance of {@link Filer} to cache records for a specified amount of time.
 *
 * @author Matt Ayres
 */
public class CachedFiler extends DecoratedFiler {
	private final LoadingCache<String, List<Record>> dirCache;
	private final LoadingCache<String, Record> fileCache;

	public CachedFiler(@Nonnull Filer delegate, @Nonnull Config config) {
		super(checkNotNull(delegate));
		checkNotNull(config);

		int concurrency = config.getInt("cache.concurrency", 4);
		long expireTime = config.getTime("cache.expireTime", "1m");

		dirCache = Caches.build((CheckedFunction<String, List<Record>, Exception>) delegate::listRecords,
				b -> b.softValues().concurrencyLevel(concurrency).expireAfterWrite(expireTime, TimeUnit.MILLISECONDS));
		fileCache = Caches.build((CheckedFunction<String, Record, Exception>) delegate::getRecord,
				b -> b.softValues().concurrencyLevel(concurrency).expireAfterWrite(expireTime, TimeUnit.MILLISECONDS));
	}

	@Override
	@Nonnull
	public List<Record> listRecords(@Nonnull String path) throws IOException {
		try {
			return dirCache.get(path);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof IOException) {
				throw (IOException) e.getCause();
			} else {
				throw new IOException(e);
			}
		}
	}

	@Override
	@Nonnull
	public Record getRecord(@Nonnull String path) throws IOException {
		String parent = new File(path).getParent();
		if (parent != null) {
			for (Record record : listRecords(parent)) {
				if (path.equals(record.getPath())) {
					return record;
				}
			}
		}

		try {
			return fileCache.get(path);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof IOException) {
				throw (IOException) e.getCause();
			} else {
				throw new IOException(e);
			}
		}
	}

	@Override
	public void createDirs(@Nonnull String path) throws IOException {
		checkNotNull(path);
		if (!getRecord(path).exists()) {
			super.createDirs(path);
			dirCache.invalidate(path);
		}
	}
}
