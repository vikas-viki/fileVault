import { Injectable, OnModuleDestroy } from "@nestjs/common";
import Redis from "ioredis";
import { ConfigService } from "@nestjs/config";


export interface RedisService extends Redis {
    selectAndReserve(...args: (string | number)[]): Promise<string[]>;
    allocateChunk(...args: (string | number)[]): Promise<string[]>;
}
  

@Injectable()
export class RedisService extends Redis implements OnModuleDestroy, OnModuleDestroy {
    constructor(
        configService: ConfigService
    ){
        const redisHost = configService.get<string>('REDIS_HOST');
        const redisPort = configService.get<number>('REDIS_PORT'); 

        if(!redisHost || !redisPort){
            throw new Error('Redis credentials not found');
        }

        super({
            host: redisHost,
            port: redisPort
        });

        this.defineCommand("allocateChunk", {
            lua: `
                -- KEYS[1] = CURRENT_BIN_FILE_KEY
                -- KEYS[2] = CURRENT_BIN_FILE_OFFSET_KEY
                -- ARGV[1] = totalBytes
                -- ARGV[2] = BIN_FILE_SIZE

                local file = redis.call('GET', KEYS[1])
                if not file then
                    return { "NEW_FILE_NEEDED", "" }
                end

                local currentOffset = tonumber(redis.call('GET', KEYS[2]) or "0")
                local maxAllowed = tonumber(ARGV[2])
                local bytesRequested = tonumber(ARGV[1])

                if (currentOffset + bytesRequested) > maxAllowed then
                    return { "NEW_FILE_NEEDED", "" }
                end

                redis.call('INCRBY', KEYS[2], bytesRequested)

                return { file, tostring(currentOffset) }
            `,
            numberOfKeys: 2
        });

        this.defineCommand('selectAndReserve', { 
            lua: `
                local n = #KEYS
                if n == 0 then return {} end
                local required = tonumber(ARGV[1]) + tonumber(ARGV[2])
                local repCount = tonumber(ARGV[3])
                local start = redis.call('INCR', ARGV[4])
                local selected = {}
                for i = 0, n - 1 do
                local key = KEYS[(start + i) % n + 1]
                local avail = tonumber(redis.call('HGET', key, 'spaceAvailableInBytes') or '0')
                local alloc = tonumber(redis.call('HGET', key, 'allocatedSpaceInBytes') or '0')
                if (avail - alloc) > required then
                    selected[#selected + 1] = key
                    if #selected == repCount then break end
                end
                end
                if #selected < repCount then return {} end
                for i = 1, #selected do
                redis.call('HINCRBY', selected[i], 'allocatedSpaceInBytes', ARGV[1])
                end
                return selected
            `
        });
    }

    onModuleInit(){
        this.on('connect', () => console.log('Redis connected successfully'));
        this.on('error', (err)=> console.error('Redis client error', err));
    }

    onModuleDestroy() {
        this.quit();
    }
}