import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from '@mui/material/styles';
import { CssBaseline, Box } from '@mui/material';
import theme from './theme';
import TopBar from './components/TopBar';
import StepperNav from './components/StepperNav';
import Home from './pages/Home';
import ShipmentUpload from './pages/ShipmentUpload';
import ShipmentListing from './pages/ShipmentListing';
import VehicleTypes from './pages/VehicleTypes';
import Configuration from './pages/Configuration';
import Solution from './pages/Solution';

const App: React.FC = () => {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
        <Box sx={{ display: 'flex' }}>
          <TopBar />
          <StepperNav />
          <Box sx={{ flexGrow: 1, ml: '280px' }}>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/shipment" element={<ShipmentListing />} />
              <Route path="/shipment/upload" element={<ShipmentUpload />} />
              <Route path="/vehicles" element={<VehicleTypes />} />
              <Route path="/configuration" element={<Configuration />} />
              <Route path="/solution" element={<Solution />} />
            </Routes>
          </Box>
        </Box>
      </Router>
    </ThemeProvider>
  );
};

export default App; 