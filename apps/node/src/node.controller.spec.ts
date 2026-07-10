import { Test, TestingModule } from '@nestjs/testing';
import { NodeController } from './node.controller';
import { NodeService } from './node.service';

describe('NodeController', () => {
  let nodeController: NodeController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [NodeController],
      providers: [NodeService],
    }).compile();

    nodeController = app.get<NodeController>(NodeController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(nodeController.getHello()).toBe('Hello World!');
    });
  });
});
