import js from '@eslint/js';
import eslintPluginSvelte from 'eslint-plugin-svelte';
import globals from 'globals';

export default [
    js.configs.recommended,
    ...eslintPluginSvelte.configs['flat/recommended'],
    {
        languageOptions: {
            ecmaVersion: 2022,
            sourceType: 'module',
            globals: {
                ...globals.browser,
                ...globals.es2021
            }
        },
        rules: {
            // Customize your rules here
        }
    },
    {
        ignores: ['node_modules/**', 'public/**', 'assets/**', '.svelte-kit/**', 'dist/**', 'static/**']
    }
];
