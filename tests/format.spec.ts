import { describe, expect, it } from 'vitest';
import Parser from 'tree-sitter';
import Language from 'tree-sitter-sql-bigquery';
import { fixDestinationSQL } from '../src/commands/fix.js';
import * as fs from 'fs';


describe('Command test: format command', () => {
  const cases: Array<{
    input: [string, string];
    expected: string;
  }> = [
      {
        input: [
          'awesome-project.sandbox.hoge',
          'create table\n`awesome-project.sandbox.wrong_name` as select 1',
        ],
        expected: 'create table\n`awesome-project.sandbox.hoge` as select 1',
      },
      {
        input: [
          'awesome-project.sandbox',
          'create schema `awesome-project.wrong_name`',
        ],
        expected: 'create schema `awesome-project.sandbox`',
      },
      {
        input: [
          'awesome-project.sandbox.hoge',
          'create or replace function `sandbox.wrong_name`() as (1)',
        ],
        expected: 'create or replace function `awesome-project.sandbox.hoge`() as (1)',
      },
      {
        input: [
          'awesome-project.sandbox.hoge',
          'create or replace table function awesomeproject.wrong_name() as (select 1)',
        ],
        expected: 'create or replace table function `awesome-project.sandbox.hoge`() as (select 1)',
      },
      {
        input: [
          'awesome-project.sandbox.correct_name',
          `create or replace procedure sandbox.wrong_name(in argument int64)
            options(description="test")
            begin select 1; end`,
        ],
        expected: `create or replace procedure \`awesome-project.sandbox.correct_name\`(in argument int64)
            options(description="test")
            begin select 1; end`,
      },
      {
        input: [
          'sandbox.correct_name',
          fs.readFileSync('tests/__sql__/example1/input.sql', 'utf8'),
        ],
        expected: fs.readFileSync('tests/__sql__/example1/expected.sql', 'utf8'),
      }
    ];
  it.each(cases)('Example SQL %#', async (args) => {
    const { input, expected } = args;
    const parser = new Parser();
    parser.setLanguage(Language);

    expect(fixDestinationSQL(parser, ...input))
      .toMatchObject(expected);
  });
});
