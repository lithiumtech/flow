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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.Comparator;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Details for a single file from a {@link Filer}.
 *
 * @author Matt Ayres
 */
public class Record implements Serializable {
	private static final long serialVersionUID = -4759712231322406072L;

	public static final long NO_EXIST_SIZE = -1;

	private final URI uri;
	private final String parent;
	private final String name;
	private final long time;
	private final long size;
	private final boolean dir;
	private volatile transient String path;

	public Record(@Nonnull URI uri, @Nullable String parent, @Nonnull String name, long time, long size, boolean dir) {
		checkNotNull(name);
		checkArgument(!name.contains("/"), "name cannot include '/' in it: %s", name);
		this.uri = checkNotNull(uri);
		this.parent = parent;
		this.name = name;
		this.time = time;
		this.size = size;
		this.dir = dir;
	}

	@Nonnull
	public URI getUri() {
		return uri;
	}

	@Nonnull
	public Optional<String> getParent() {
		return Optional.ofNullable(parent);
	}

	@Nonnull
	public String getName() {
		return name;
	}

	@Nonnull
	public String getPath() {
		if (path == null) {
			path = parent == null ? "/" : parent.equals("/") ? "/" + name : parent + "/" + name;
		}
		return path;
	}

	public long getTime() {
		return time;
	}

	public long getSize() {
		return size;
	}

	public boolean exists() {
		return size != NO_EXIST_SIZE;
	}

	public boolean isDir() {
		return dir;
	}

	public boolean isFile() {
		return !dir;
	}

	@Nonnull
	public Record withParent(@Nullable String newParent) {
		return new Record(uri, newParent, name, time, size, dir);
	}

	@Nonnull
	public Record withName(@Nonnull String newName) {
		return new Record(uri, parent, newName, time, size, dir);
	}

	@Nonnull
	public Record withSize(long newSize) {
		return new Record(uri, parent, name, time, newSize, dir);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Record that = (Record) o;
		return dir == that.dir && size == that.size && time == that.time && uri.equals(that.uri)
				&& name.equals(that.name);
	}

	@Override
	public int hashCode() {
		int result = uri.hashCode();
		result = 31 * result + name.hashCode();
		result = 31 * result + (int) (time ^ (time >>> 32));
		result = 31 * result + (int) (size ^ (size >>> 32));
		result = 31 * result + (dir ? 1 : 0);
		return result;
	}

	@Override
	@Nonnull
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	@Nonnull
	public static Record noFile(@Nonnull URI uri, @Nonnull String path) {
		checkNotNull(uri);
		checkNotNull(path);
		File file = new File(path);
		return new Record(uri, file.getParent(), file.getName(), 0, NO_EXIST_SIZE, false);
	}

	@Nonnull
	public static Comparator<Record> sizeAsc() {
		return Comparator.comparingLong(Record::getSize);
	}

	@Nonnull
	public static Comparator<Record> sizeDesc() {
		return Comparator.comparingLong(Record::getSize).reversed();
	}

	@Nonnull
	public static Comparator<Record> timeAsc() {
		return Comparator.comparingLong(Record::getTime);
	}

	@Nonnull
	public static Comparator<Record> timeDesc() {
		return Comparator.comparingLong(Record::getTime).reversed();
	}

	@Nonnull
	public static Comparator<Record> nameAsc() {
		return Comparator.comparing(Record::getName);
	}

	@Nonnull
	public static Comparator<Record> nameDesc() {
		return Comparator.comparing(Record::getName).reversed();
	}
}
