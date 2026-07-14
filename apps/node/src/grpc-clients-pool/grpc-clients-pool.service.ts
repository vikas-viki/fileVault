import { NODE, STREAM_CHUNK_SIZE } from '@app/shared/helpers/constants';
import { connectivityState } from '@grpc/grpc-js';
import { Injectable } from '@nestjs/common';
import { ClientGrpcProxy, ClientProxyFactory, Transport } from "@nestjs/microservices";
import path from 'path';

@Injectable()
export class GrpcClientsPoolService {
    constructor() { }

    private clientsPool = new Map<string, ClientGrpcProxy>();

    async getClient(targetUrl: string): Promise<ClientGrpcProxy | null> {
        try {
            const existingClient = this.clientsPool.get(targetUrl);
            if (existingClient) {
                const connection = (existingClient as any).grpcClient;
                const channel = connection.channel;

                if(channel){
                    const state = channel.getConnectivityState(false);
                    if(state === connectivityState.SHUTDOWN || state === connectivityState.TRANSIENT_FAILURE){
                        console.warn(`${NODE} existing connection to ${targetUrl} is closed creating a new one`);
                        try {
                            existingClient.close();
                        } catch (e) {}
                        this.clientsPool.delete(targetUrl);
                    }else {
                        return existingClient;
                    }
                }
            }

            console.log(`${NODE} creating brand new client for ${targetUrl}`);
            const newClient = ClientProxyFactory.create({
                transport: Transport.GRPC,
                options: {
                    url: targetUrl,
                    package: 'node',
                    protoPath: path.join(__dirname, '../../libs/shared/protos/node.proto'),
                    loader: {
                        keepCase: true
                    },
                    channelOptions: {
                        // Ping the node every 10 seconds to make sure it's alive
                        'grpc.keepalive_time_ms': 10000,
                        // Wait 5 seconds for a response to the ping before dropping the connection
                        'grpc.keepalive_timeout_ms': 5000,
                        // Allow keepalive pings even if there are no active streams
                        'grpc.keepalive_permit_without_calls': 1,
                        "grpc.max_send_message_length": STREAM_CHUNK_SIZE,
                        "grpc.max_receive_message_length": STREAM_CHUNK_SIZE
                    }
                }
            });

            this.clientsPool.set(targetUrl, newClient);
            console.log(`${NODE} new client created for ${targetUrl}`);
            return newClient;
        } catch (err) {
            console.error(`${NODE} error getting grpc client: `, err);
            return null;
        }
    }

    async onModuleDestroy() {
        try {
            for (const [url, client] of this.clientsPool.entries()) {
                client.close();
                console.log(`${NODE} client connection closed: `, url)
            }
        } catch (err) {
            console.error(`${NODE} error closing connection to nodes: `, err);
        }
    }
}
