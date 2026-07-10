import { Test, TestingModule } from '@nestjs/testing';
import { CoordinatorController } from './coordinator.controller';
import { CoordinatorService } from './coordinator.service';

describe('CoordinatorController', () => {
  let coordinatorController: CoordinatorController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [CoordinatorController],
      providers: [CoordinatorService],
    }).compile();

    coordinatorController = app.get<CoordinatorController>(CoordinatorController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(coordinatorController.getHello()).toBe('Hello World!');
    });
  });
});
