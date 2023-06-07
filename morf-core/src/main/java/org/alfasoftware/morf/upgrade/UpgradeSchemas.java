package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.metadata.Schema;

/**
 * Composite object to hold source and target schemas.
 */
public class UpgradeSchemas {

    /**
     * Object to hold schema pre upgrade.
     */
    private Schema sourceSchema;

    /**
     * Object to hold target schema for upgrade.
     */
    private Schema targetSchema;

    public UpgradeSchemas(Schema sourceSchema, Schema targetSchema) {
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
    }

    public Schema getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(Schema sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public Schema getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(Schema targetSchema) {
        this.targetSchema = targetSchema;
    }
}
