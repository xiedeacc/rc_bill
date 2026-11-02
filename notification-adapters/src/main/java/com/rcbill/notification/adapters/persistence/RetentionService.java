package com.rcbill.notification.adapters.persistence;

/** Deletes only fully terminal chains after every retention and publication obligation expires. */
public final class RetentionService {
    private final Shards shards;
    public RetentionService(Shards shards){this.shards=shards;}
    public int purge(int batch){
        int purged=0;
        for(var shard:shards.all())purged+=shard.transaction(m->{
            m.purgePayload(batch);m.purgeOutbox(batch);var roots=m.expiredChains(Math.min(batch,20));
            for(var row:roots){var root=Db.id(row.get("root_id"));m.deleteAudit(root);m.deletePayload(root);m.deleteAttempts(root);m.deleteDedup(root);m.deleteOutbox(root);m.deleteTasks(root);m.deleteChain(root);}
            return roots.size();
        });return purged;
    }
}
