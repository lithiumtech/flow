package com.lithium.flow.paramstore;

import com.lithium.flow.access.Access;
import com.lithium.flow.config.Config;
import com.lithium.flow.shell.Shells;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by karan.shah on 8/1/18.
 */
public class SSM {
    @Nonnull
    public static Access buildAccess(@Nonnull Config config) {
        checkNotNull(config);
        return Shells.buildAccess(config);
    }
}
