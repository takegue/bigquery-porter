import { describe, expect, it } from 'vitest';
import Parser from 'tree-sitter';
import Language from 'tree-sitter-sql-bigquery';
import { fixDestinationSQL } from '../src/commands/fix.js';
import * as fs from 'fs';

describe('fixDestinationSQL', async () => {
  const cases: Array<{
    input: ['table_or_routine' | 'dataset' | 'project', string, string];
    expected: string;
  }> = [
    {
      input: [
        'table_or_routine',
        'awesome-project.sandbox.hoge',
        'create table\n`awesome-project.sandbox.wrong_name` as select 1',
      ],
      expected: 'create table\n`awesome-project.sandbox.hoge` as select 1',
    },
    {
      input: [
        'dataset',
        'awesome-project.sandbox',
        'create schema `awesome-project.wrong_name`',
      ],
      expected: 'create schema `awesome-project.sandbox`',
    },
    {
      input: [
        'table_or_routine',
        'awesome-project.sandbox.hoge',
        'create or replace function `sandbox.wrong_name`() as (1)',
      ],
      expected:
        'create or replace function `awesome-project.sandbox.hoge`() as (1)',
    },
    {
      input: [
        'table_or_routine',
        'awesome-project.sandbox.hoge',
        'create or replace table function awesomeproject.wrong_name() as (select 1)',
      ],
      expected:
        'create or replace table function `awesome-project.sandbox.hoge`() as (select 1)',
    },
    {
      input: [
        'table_or_routine',
        'awesome-project.sandbox.correct_name',
        `create or replace procedure sandbox.wrong_name(in argument int64)
            options(description="test")
            begin select 1; end`,
      ],
      expected:
        `create or replace procedure \`awesome-project.sandbox.correct_name\`(in argument int64)
            options(description="test")
            begin select 1; end`,
    },
    {
      input: [
        'dataset',
        'awesome-project.sandbox',
        'call `v0.test.procedure()`',
      ],
      expected: 'call `v0.test.procedure()`',
    },
    {
      input: [
        'table_or_routine',
        'sandbox.correct_name',
        fs.readFileSync('tests/__sql__/example1/input.sql', 'utf8'),
      ],
      expected: fs.readFileSync('tests/__sql__/example1/expected.sql', 'utf8'),
    },
    {
      input: [
        'table_or_routine',
        'replaced_schema.awesome_procedure',
        fs.readFileSync('tests/__sql__/example2/input.sql', 'utf8'),
      ],
      expected: fs.readFileSync('tests/__sql__/example2/expected.sql', 'utf8'),
    },
    {
      input: [
        'dataset',
        'replaced_schema',
        fs.readFileSync('tests/__sql__/example2/input.sql', 'utf8'),
      ],
      expected: fs.readFileSync('tests/__sql__/example2/expected2.sql', 'utf8'),
    },
    {
      input: [
        'table_or_routine',
        'missing_table',
        fs.readFileSync('tests/__sql__/example2/input.sql', 'utf8'),
      ],
      expected: fs.readFileSync('tests/__sql__/example2/input.sql', 'utf8'),
    },
    {
      input: [
        'table_or_routine',
        'v0.hoge',
        fs.readFileSync('tests/__sql__/example3/input.sql', 'utf8'),
      ],
      expected: fs.readFileSync('tests/__sql__/example3/expected.sql', 'utf8'),
    },
    {
      input: [
        'table_or_routine',
        'v0.hoge',
        fs.readFileSync('tests/__sql__/example4/input.sql', 'utf8'),
      ],
      expected: fs.readFileSync('tests/__sql__/example4/expected.sql', 'utf8'),
    },
  ];
  it.concurrent.each(cases)('Example SQL %#', async (args) => {
    const { input, expected } = args;
    const parser = new Parser();
    parser.setLanguage(Language);

    expect(fixDestinationSQL(parser, ...input))
      .toMatchObject(expected);
  });

  it.concurrent.each(cases)(
    'Stablity check: multiple formatting %#',
    async (args) => {
      const { input } = args;
      const parser = new Parser();
      parser.setLanguage(Language);

      expect(fixDestinationSQL(parser, ...input))
        .toMatchObject(
          fixDestinationSQL(
            parser,
            input[0],
            input[1],
            fixDestinationSQL(parser, ...input),
          ),
        );
    },
  );
});
