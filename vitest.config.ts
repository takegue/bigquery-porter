import { defineConfig } from 'vite';
export default defineConfig({
  test: {
    threads: true,
    maxThreads: 1,
    minThreads: 0,
  },
});
