import { Body, Controller, Post, Res } from '@nestjs/common';
import { AuthService } from './auth.service';
import { GoogleAuthDto } from './auth.dto';

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) { }

  @Post('register')
  async register(@Body() data: GoogleAuthDto, @Res({ passthrough: true }) response) {
    let { token, ...responseData } = await this.authService.register(data);
    this.authService.setCookie(response, token);
    return responseData;
  }

  @Post('login')
  async login(@Body() data: GoogleAuthDto, @Res({ passthrough: true }) response) {
    let { token, ...responseData } = await this.authService.register(data);
    this.authService.setCookie(response, token);
    return responseData;
  }
}
