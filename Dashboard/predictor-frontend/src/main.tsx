import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { PicksProvider } from './contexts/PicksContext'
import { AgentScreenProvider } from './contexts/AgentScreenContext'
import { AgentChatProvider } from './contexts/AgentChatContext'

// AgentScreenProvider wraps AgentChatProvider: the chat reads the current screen
// descriptor when a prompt is submitted, so the screen context must exist first.
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PicksProvider>
      <AgentScreenProvider>
        <AgentChatProvider>
          <App />
        </AgentChatProvider>
      </AgentScreenProvider>
    </PicksProvider>
  </StrictMode>,
)
