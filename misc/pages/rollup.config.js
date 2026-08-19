import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import postcss from 'rollup-plugin-postcss';
import copy from 'rollup-plugin-copy';
import svelte from 'rollup-plugin-svelte';

export default [
  // Global bundle (Bootstrap, FontAwesome)
  {
    input: 'main.js',
    output: {
      file: 'static/generated/bundle.js',
      format: 'iife',
      name: 'GlobalBundle'
    },
    plugins: [
      nodeResolve(),
      commonjs(),
      postcss({
        extract: true,
        minimize: true
      }),
      copy({
        targets: [
          { src: 'node_modules/@fortawesome/fontawesome-free/webfonts', dest: 'static' }
        ]
      })
    ]
  },
  // Playground bundle (Svelte App)
  {
    input: './svelte/playground/main.js',
    output: {
      file: 'static/generated/playground.js',
      format: 'iife',
      name: 'PlaygroundApp'
    },
    plugins: [
      svelte({
        emitCss: true,
        compilerOptions: {
          dev: false
        }
      }),
      nodeResolve({
        browser: true,
        exportConditions: ['svelte']
      }),
      commonjs(),
      postcss({
        extract: true,
        minimize: true
      })
    ],
    onwarn: (warning, warn) => {
      // Suppress internal Svelte circular dependency warnings
      if (warning.code === 'CIRCULAR_DEPENDENCY' && (warning.importer?.includes('node_modules/svelte') || warning.message.includes('node_modules/svelte'))) {
        return;
      }
      warn(warning);
    }
  },
];
