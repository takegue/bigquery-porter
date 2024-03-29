import { DefaultReporter } from './default.js';
import { JSONReporter } from './json.js';

export const ReporterMap = {
  'default': DefaultReporter,
  'console': DefaultReporter,
  'json': JSONReporter,
};

// export type BuiltInReporters = keyof typeof ReporterMap;
export type BuiltInReporters = 'console' | 'default';
