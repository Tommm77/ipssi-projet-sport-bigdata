import React, { useState } from 'react';
import { Routes, Route } from 'react-router-dom';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';

// Composants
import Header from './components/Header';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import SportsPage from './pages/SportsPage';
import MatchsPage from './pages/MatchsPage';
import UsersPage from './pages/UsersPage';
import NotificationsPage from './pages/NotificationsPage';
import PipelinePage from './pages/PipelinePage';

// Style
import { Box, Container } from '@mui/material';

const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
    background: {
      default: '#f5f5f5',
    },
  },
});

function App() {
  const [drawerOpen, setDrawerOpen] = useState(true);

  const toggleDrawer = () => {
    setDrawerOpen(!drawerOpen);
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex' }}>
        <Header toggleDrawer={toggleDrawer} open={drawerOpen} />
        <Sidebar open={drawerOpen} />
        <Box
          component="main"
          sx={{
            flexGrow: 1,
            p: 3,
            width: { sm: `calc(100% - ${drawerOpen ? 240 : 0}px)` },
            ml: { sm: `${drawerOpen ? 240 : 0}px` },
            mt: '64px',
            transition: 'margin 0.2s'
          }}
        >
          <Container maxWidth="xl">
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/sports" element={<SportsPage />} />
              <Route path="/matchs" element={<MatchsPage />} />
              <Route path="/users" element={<UsersPage />} />
              <Route path="/notifications" element={<NotificationsPage />} />
              <Route path="/pipeline" element={<PipelinePage />} />
            </Routes>
          </Container>
        </Box>
      </Box>
    </ThemeProvider>
  );
}

export default App; 