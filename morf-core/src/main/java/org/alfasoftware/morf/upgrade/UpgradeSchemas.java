package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.metadata.Schema;

/**
 * Composite object to hold source and target schemas.
 */
public final class UpgradeSchemas {

    /**
     * Object to hold schema pre upgrade.
     */
    private final Schema sourceSchema;

    /**
     * Object to hold target schema for upgrade.
     */
    private final Schema targetSchema;

    public UpgradeSchemas(Schema sourceSchema, Schema targetSchema) {
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
    }

    public UpgradeSchemas() {
        this.sourceSchema = null;
        this.targetSchema = null;
    }

    public Schema getSourceSchema() {
        return sourceSchema;
    }

    public Schema getTargetSchema() {
        return targetSchema;
    }
}
