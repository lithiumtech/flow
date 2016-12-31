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

package com.lithium.flow.shell;

import static com.google.common.base.Preconditions.checkNotNull;

import com.lithium.flow.access.Access;
import com.lithium.flow.access.Prompt;
import com.lithium.flow.config.Config;
import com.lithium.flow.ioc.Locator;
import com.lithium.flow.shell.cache.CachedShore;
import com.lithium.flow.shell.sshj.SshjShore;
import com.lithium.flow.shell.tunnel.AutoTunneler;
import com.lithium.flow.shell.tunnel.CachedTunneler;
import com.lithium.flow.shell.tunnel.ShellTunneler;

import javax.annotation.Nonnull;

/**
 * @author Matt Ayres
 */
public class Shells {
	@Nonnull
	public static Access buildAccess(@Nonnull Config config) {
		checkNotNull(config);
		Prompt prompt = (name, message, mask, retry) -> "";
		return new ShellAccess(config, prompt);
	}

	@Nonnull
	public static Shore buildShore(@Nonnull Locator locator) {
		checkNotNull(locator);
		return buildShore(locator.getInstance(Config.class), locator.getInstance(Access.class));
	}

	@Nonnull
	public static Shore buildShore(@Nonnull Config config, @Nonnull Access access) {
		checkNotNull(config);
		checkNotNull(access);

		Shore shore = new SshjShore(config, access);
		if (config.getBoolean("shell.cache", false)) {
			shore = new CachedShore(shore, config, access);
		}
		return shore;
	}

	@Nonnull
	public static Tunneler buildTunneler(@Nonnull Config config, @Nonnull Access access) {
		checkNotNull(config);
		checkNotNull(access);
		return buildTunneler(config, access, buildShore(config, access));
	}

	@Nonnull
	public static Tunneler buildTunneler(@Nonnull Config config, @Nonnull Access access, @Nonnull Shore shore) {
		checkNotNull(config);
		checkNotNull(access);
		checkNotNull(shore);

		Tunneler tunneler = new ShellTunneler(config, access, shore);
		if (config.getBoolean("tunnel.auto", false)) {
			tunneler = new AutoTunneler(tunneler, config.getTime("tunnel.auto.timeout", "5s"));
		}
		if (config.getBoolean("tunnel.cache", true)) {
			tunneler = new CachedTunneler(config, tunneler);
		}
		return tunneler;
	}
}
