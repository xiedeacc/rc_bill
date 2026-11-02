package com.rcbill.notification.adapters.persistence;

import java.util.*;
import org.apache.ibatis.annotations.*;

/** All statements are fixed SQL with bound values, executed inside a single routed datasource. */
public interface LedgerMapper {
    @Select("SELECT clock_timestamp()") java.time.Instant now();
    @Select("SELECT * FROM notification WHERE id=#{id}") Map<String,Object> task(@Param("id") UUID id);
    @Select("SELECT * FROM notification WHERE id=#{id} AND tenant_id=#{tenant} AND client_id=#{client}") Map<String,Object> ownedTask(Map<String,Object> args);
    @Select("SELECT * FROM notification WHERE id=#{id} FOR UPDATE") Map<String,Object> lockTask(@Param("id") UUID id);
    @Select("SELECT d.*, n.created_at FROM idempotency_record d JOIN notification n ON n.id=d.notification_id WHERE d.tenant_id=#{tenant} AND d.client_id=#{client} AND d.scope=#{scope} AND d.key=#{key}")
    Map<String,Object> dedup(Map<String,Object> args);
    @Insert("""
        INSERT INTO notification(id,tenant_id,client_id,slot,destination_id,destination_version,snapshot,fingerprint,root_id,replay_of,delivery_key,status,expires_at)
        VALUES(#{id},#{tenant},#{client},#{slot},#{destination},#{version},#{snapshot},#{fingerprint},#{root},#{replayOf},#{deliveryKey},'PENDING',#{expires})
        """) int insertTask(Map<String,Object> args);
    @Insert("INSERT INTO payload(notification_id,ciphertext) VALUES(#{id},#{cipher})") int insertPayload(Map<String,Object> args);
    @Select("SELECT ciphertext FROM payload WHERE notification_id=#{id}") Map<String,Object> payloadRow(@Param("id") UUID id);
    default byte[] payload(UUID id) {var row=payloadRow(id);return row==null?null:(byte[])row.get("ciphertext");}
    @Insert("INSERT INTO idempotency_record(tenant_id,client_id,scope,key,fingerprint,notification_id) VALUES(#{tenant},#{client},#{scope},#{key},#{fingerprint},#{id}) ON CONFLICT DO NOTHING")
    int insertDedup(Map<String,Object> args);
    @Insert("INSERT INTO delivery_chain(root_id,active_id) VALUES(#{id},#{id})") int insertChain(@Param("id") UUID id);
    @Select("SELECT * FROM delivery_chain WHERE root_id=#{id} FOR UPDATE") Map<String,Object> lockChain(@Param("id") UUID id);
    @Update("UPDATE delivery_chain SET active_id=#{active} WHERE root_id=#{root}") int active(Map<String,Object> args);
    @Insert("INSERT INTO replay_audit(id,original_id,replay_id,actor,reason,risk_acknowledged) VALUES(#{auditId},#{original},#{id},#{actor},#{reason},#{risk})") int audit(Map<String,Object> args);
    @Insert("""
        INSERT INTO outbox_event(event_id,notification_id,slot,schedule_epoch,event_type,available_at)
        VALUES(#{event},#{id},#{slot},#{epoch},#{type},#{available}) ON CONFLICT(notification_id,schedule_epoch,event_type) DO NOTHING
        """) int outbox(Map<String,Object> args);
    @Update("""
        UPDATE notification SET status='IN_FLIGHT',attempt_count=attempt_count+1,lease_token=#{token},lease_until=#{until}
        WHERE id=#{id} AND schedule_epoch=#{epoch} AND status IN ('PENDING','RETRY_WAIT')
        """) int claim(Map<String,Object> args);
    @Insert("INSERT INTO delivery_attempt(notification_id,attempt_no,lease_token,outcome) VALUES(#{id},#{attempt},#{token},'STARTED')") int start(Map<String,Object> args);
    @Update("""
        UPDATE notification SET status=#{status},schedule_epoch=#{epoch},next_attempt_at=#{next},last_error=#{error},
        completed_at=#{completed},lease_token=NULL,lease_until=NULL WHERE id=#{id}
        """) int transition(Map<String,Object> args);
    @Update("""
        UPDATE delivery_attempt SET finished_at=clock_timestamp(),outcome=#{outcome},http_status=#{http},error_code=#{error},elapsed_ms=#{elapsed},provider_request_id=#{provider}
        WHERE notification_id=#{id} AND lease_token=#{token} AND outcome='STARTED'
        """) int finish(Map<String,Object> args);
    @Select("SELECT a.* FROM delivery_attempt a JOIN notification n ON n.id=a.notification_id WHERE a.notification_id=#{id} AND n.tenant_id=#{tenant} AND n.client_id=#{client} AND a.attempt_no>#{after} ORDER BY a.attempt_no LIMIT #{limit}")
    List<Map<String,Object>> attempts(Map<String,Object> args);
    @Select("SELECT * FROM notification WHERE status='IN_FLIGHT' AND lease_until<clock_timestamp() ORDER BY lease_until LIMIT #{limit} FOR UPDATE SKIP LOCKED")
    List<Map<String,Object>> abandoned(@Param("limit") int limit);
    @Select("SELECT * FROM notification WHERE status IN ('PENDING','RETRY_WAIT') AND expires_at<=clock_timestamp() ORDER BY expires_at LIMIT #{limit} FOR UPDATE SKIP LOCKED")
    List<Map<String,Object>> expired(@Param("limit") int limit);
    @Select("""
        SELECT o.*,n.destination_id,n.expires_at FROM outbox_event o JOIN notification n ON n.id=o.notification_id
        WHERE o.publish_state='PENDING' AND o.available_at<=clock_timestamp()
        ORDER BY o.available_at,o.event_id LIMIT #{limit} FOR UPDATE OF o SKIP LOCKED
        """) List<Map<String,Object>> dueOutbox(@Param("limit") int limit);
    @Update("UPDATE outbox_event SET publish_state='IN_FLIGHT',lease_token=#{token},lease_until=#{until} WHERE event_id=#{event}") int leaseOutbox(Map<String,Object> args);
    @Update("UPDATE outbox_event SET publish_state='PUBLISHED',published_at=clock_timestamp(),lease_token=NULL,lease_until=NULL WHERE event_id=#{event} AND lease_token=#{token} AND publish_state='IN_FLIGHT'") int published(Map<String,Object> args);
    @Update("UPDATE outbox_event SET publish_state='PENDING',available_at=#{available},lease_token=NULL,lease_until=NULL WHERE event_id=#{event} AND lease_token=#{token} AND publish_state='IN_FLIGHT'") int republish(Map<String,Object> args);
    @Update("UPDATE outbox_event SET publish_state='PENDING',lease_token=NULL,lease_until=NULL WHERE event_id IN (SELECT event_id FROM outbox_event WHERE publish_state='IN_FLIGHT' AND lease_until<clock_timestamp() LIMIT #{limit} FOR UPDATE SKIP LOCKED)") int recoverOutbox(@Param("limit") int limit);
    @Insert("INSERT INTO topology_guard(singleton,shard_count,partition_count) VALUES(true,#{shards},#{partitions}) ON CONFLICT DO NOTHING") int initializeTopology(Map<String,Object> args);
    @Select("SELECT * FROM topology_guard WHERE singleton=true") Map<String,Object> topology();
    @Insert("INSERT INTO destination_version(destination_id,version,fingerprint) VALUES(#{id},#{version},#{fingerprint}) ON CONFLICT DO NOTHING") int registerDestination(Map<String,Object> args);
    @Select("SELECT fingerprint FROM destination_version WHERE destination_id=#{id} AND version=#{version}") String destinationFingerprint(Map<String,Object> args);
    @Select("SELECT min(created_at) FROM outbox_event WHERE publish_state<>'PUBLISHED'") java.time.Instant oldestUnpublished();
    @Delete("DELETE FROM payload WHERE notification_id IN (SELECT n.id FROM notification n JOIN payload p ON p.notification_id=n.id WHERE n.completed_at<clock_timestamp()-interval '7 days' ORDER BY n.completed_at,n.id LIMIT #{limit})") int purgePayload(@Param("limit") int limit);
    @Delete("DELETE FROM outbox_event WHERE event_id IN (SELECT event_id FROM outbox_event WHERE publish_state='PUBLISHED' AND published_at<clock_timestamp()-interval '1 day' LIMIT #{limit})") int purgeOutbox(@Param("limit") int limit);
    @Select("""
        SELECT c.root_id FROM delivery_chain c WHERE c.active_id IS NULL
        AND NOT EXISTS (SELECT 1 FROM notification n WHERE n.root_id=c.root_id AND (n.completed_at IS NULL OR n.completed_at>=clock_timestamp()-interval '30 days'))
        AND NOT EXISTS (SELECT 1 FROM idempotency_record d JOIN notification n ON n.id=d.notification_id WHERE n.root_id=c.root_id AND d.retain_until>=clock_timestamp())
        AND NOT EXISTS (SELECT 1 FROM outbox_event o JOIN notification n ON n.id=o.notification_id WHERE n.root_id=c.root_id AND o.publish_state<>'PUBLISHED')
        ORDER BY c.root_id LIMIT #{limit} FOR UPDATE OF c SKIP LOCKED
        """) List<Map<String,Object>> expiredChains(@Param("limit") int limit);
    @Delete("DELETE FROM replay_audit WHERE original_id IN (SELECT id FROM notification WHERE root_id=#{root}) OR replay_id IN (SELECT id FROM notification WHERE root_id=#{root})") int deleteAudit(@Param("root") UUID root);
    @Delete("DELETE FROM payload WHERE notification_id IN (SELECT id FROM notification WHERE root_id=#{root})") int deletePayload(@Param("root") UUID root);
    @Delete("DELETE FROM delivery_attempt WHERE notification_id IN (SELECT id FROM notification WHERE root_id=#{root})") int deleteAttempts(@Param("root") UUID root);
    @Delete("DELETE FROM idempotency_record WHERE notification_id IN (SELECT id FROM notification WHERE root_id=#{root})") int deleteDedup(@Param("root") UUID root);
    @Delete("DELETE FROM outbox_event WHERE notification_id IN (SELECT id FROM notification WHERE root_id=#{root})") int deleteOutbox(@Param("root") UUID root);
    @Delete("DELETE FROM notification WHERE root_id=#{root}") int deleteTasks(@Param("root") UUID root);
    @Delete("DELETE FROM delivery_chain WHERE root_id=#{root}") int deleteChain(@Param("root") UUID root);
}
