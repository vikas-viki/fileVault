import { Inject, Injectable } from '@nestjs/common';
import type { AvailableNodesResponse, HeartbeatRequest, HeartbeatResponse } from './heartbeat.type';
import { AVAILABLE_NODES_KEY, HEARTBEAT_SERVICE, HEARTBEAT_TIMEOUT_SECONDS, REDIS_CLIENT } from '@app/shared/constants';
import Redis from 'ioredis';


@Injectable()
export class HeartbeatService {

    constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis){}

    async logHeartbeat(data: HeartbeatRequest): Promise<HeartbeatResponse> {
        try{
            console.log(`${HEARTBEAT_SERVICE} received heartbeat request: `, data);
            const nodeKey = `${data.ip}:::${data.port}`;
            const expirtAt = Date.now() * (HEARTBEAT_TIMEOUT_SECONDS * 1000);
            const pipeline = this.redis.multi();
            
            pipeline.hset(nodeKey, {
                spaceAvailableInBytes: data.spaceAvailableInBytes
            });
            pipeline.expire(nodeKey, HEARTBEAT_TIMEOUT_SECONDS);
            pipeline.zadd(AVAILABLE_NODES_KEY, expirtAt, nodeKey);
            
            await pipeline.exec();
            
            return {
                status: true
            };
        }catch(e){
            console.error(`${HEARTBEAT_SERVICE} error logging heartbeat: `, e);
            return {
                status: false
            };
        }
    }

    async getAvailabeNodes(): Promise<AvailableNodesResponse> {
        try{
            const deadNodes = await this.redis.zrangebyscore(AVAILABLE_NODES_KEY, '-inf', Date.now());
            
            if(deadNodes.length > 0){
                const pipeline = this.redis.multi();
                
                deadNodes.forEach(d =>{
                    pipeline.del(d);
                });
                pipeline.zremrangebyscore(AVAILABLE_NODES_KEY, '-inf', Date.now());
                await pipeline.exec();
            }

            const aliveNodes = await this.redis.zrangebyscore(AVAILABLE_NODES_KEY, 0, -1);
            return aliveNodes;
        }catch(e){
            console.error(`${HEARTBEAT_SERVICE} error getting available nodes: `, e);
            return [];
        }
    }
}
