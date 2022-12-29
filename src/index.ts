#!/usr/bin/env node
import { createCLI } from '../src/commands/cli.js';

const main = async () => {
  const cli = createCLI();
  cli.parse();
};

main();
