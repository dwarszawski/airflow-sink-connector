package com.dwarszawski.airflowsink.validators;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class EndpointValidator implements ConfigDef.Validator {
    private String[] schemes = {"https", "http"};

    @Override
    public void ensureValid(String name, Object value) {
        UrlValidator urlValidator = new UrlValidator(schemes);
        if (!urlValidator.isValid((String) value)) {
            throw new ConfigException(name, value, "Not valid url");
        }
    }
}
