import { Module } from "@nestjs/common";
import { GrpcClientsPoolService } from "./grpc-clients-pool.service";
import { GrpcRelayWriterService } from "./grpc-relay-writer.service";

@Module({
    providers: [GrpcClientsPoolService, GrpcRelayWriterService],
    exports: [GrpcClientsPoolService, GrpcRelayWriterService],
})
export class GrpcModule {}