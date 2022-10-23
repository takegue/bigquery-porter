import { defineConfig } from 'vite';
export default defineConfig({
  test: {
    // FIXME: Couldn't thread in linux due to "Module did not self-register Error"
    threads: false,
    coverage: {
      provider: 'istanbul', // or 'c8'
      reporter: ['text', 'json', 'html'],
    },
  },
});
