package com.lithium.flow.access.prompts;

import com.lithium.aws.parameterstore.ParameterStore;
import com.lithium.flow.access.Prompt;

import javax.annotation.Nonnull;
import java.util.Map;

public class AWSPrompt implements Prompt {
    private final String awsRegion;
    private final String ssmParameterPath;
    private final String configProfile;
    private final String credentialProfile;

    public AWSPrompt(@Nonnull String ssmParameterPath, @Nonnull String awsRegion, String credentialProfile, String configProfile) {
        this.awsRegion = awsRegion;
        this.ssmParameterPath = ssmParameterPath;
        this.configProfile = configProfile;
        this.credentialProfile = credentialProfile;
    }

    @Nonnull
    @Override
    public Response prompt(@Nonnull String name, @Nonnull String message, @Nonnull Type type) {
        return new Response() {
            @Nonnull
            @Override
            public String value() {
                ParameterStore parameterStore = new ParameterStore(ssmParameterPath, awsRegion, credentialProfile, configProfile);
                Map<String, Object> paramMap = parameterStore.getParamByPath();

                if (name.startsWith("key[")) {
                    String sshKey = (String) paramMap.get("sshKey");
                    return sshKey;
                } else if (name.startsWith("pass[")) {
                    String sshPassword = (String) paramMap.get("sshPassword");
                    return sshPassword;
                } else {
                    throw new RuntimeException("Not supported: " + name);
                }

            }

            @Nonnull
            @Override
            public String accept() {
                return value();
            }

            @Override
            public void reject() {

            }
        };
    }
}
