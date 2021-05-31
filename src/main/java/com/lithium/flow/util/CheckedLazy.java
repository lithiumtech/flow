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

package com.lithium.flow.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * @author Matt Ayres
 */
public class CheckedLazy<T, E extends Exception> {
	private final CheckedSupplier<T, E> supplier;
	private volatile T object;

	public CheckedLazy(@Nonnull CheckedSupplier<T, E> supplier) {
		this.supplier = checkNotNull(supplier);
	}

	@Nonnull
	public T get() throws E {
		T result = object;
		if (result == null) {
			synchronized (this) {
				result = object;
				if (result == null) {
					result = checkNotNull(supplier.get());
					object = result;
				}
			}
		}
		return result;
	}

	@Nonnull
	public Optional<T> getOptional() {
		return Optional.ofNullable(object);
	}

	public void invalidate() {
		synchronized (this) {
			object = null;
		}
	}
}
