import { mount } from 'svelte';
import Playground from './Playground.svelte';

const target = document.getElementById('svelte-app');
let app;

if (target) {
    target.innerHTML = ''; // Clear loading spinner
    try {
        app = mount(Playground, { target });
        console.log("Playground mounted successfully");
    } catch (e) {
        console.error("Failed to mount Svelte app:", e);
        target.innerHTML = `<div class="alert alert-danger m-4"><h4>Error loading playground</h4><p>${e.message}</p><pre>${e.stack}</pre></div>`;
    }
}

export default app;
