import { Global, Module } from "@nestjs/common";
import { BinFileStorageService } from "./bin-file-storage.service";

@Global()
@Module({
    providers: [BinFileStorageService],
    exports: [BinFileStorageService]
})
export class BinFileSotrageModule {}