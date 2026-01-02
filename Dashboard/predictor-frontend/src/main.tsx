import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { PicksProvider } from './contexts/PicksContext'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <PicksProvider>
      <App />
    </PicksProvider>
  </StrictMode>,
)
