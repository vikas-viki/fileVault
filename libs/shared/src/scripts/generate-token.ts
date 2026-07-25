import { signToken } from '../auth/tokens';
import { TokenScope } from '../helpers/constants';

function main() {
  const [expiresIn = '30d', sub] = process.argv.slice(2);
  console.log(signToken({ scope: TokenScope.CLIENT, sub }, expiresIn));
}

main();
