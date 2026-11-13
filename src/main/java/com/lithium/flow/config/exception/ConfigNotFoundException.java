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

package com.lithium.flow.config.exception;

import java.io.FileNotFoundException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Thrown when a config's own entry does not exist, for example because it was deleted after it was listed.
 * A missing include inside an existing config is reported as a plain {@link FileNotFoundException} instead.
 */
public class ConfigNotFoundException extends FileNotFoundException {
	public ConfigNotFoundException(@Nonnull String message) {
		super(message);
	}

	public ConfigNotFoundException(@Nonnull String message, @Nullable Throwable cause) {
		super(message);
		initCause(cause);
	}
}
