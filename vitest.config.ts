import { defineConfig } from 'vite';
export default defineConfig({
  test: {
    // FIXME: Couldn't thread in linux due to "Module did not self-register Error"
    threads: false,
    testTimeout: 5000,
    coverage: {
      provider: 'istanbul',
      reporter: ['text', 'json', 'html'],
    },
  },
});
