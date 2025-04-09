import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
  Drawer,
  Divider,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Toolbar,
  ListItemButton
} from '@mui/material';
import DashboardIcon from '@mui/icons-material/Dashboard';
import SportsSoccerIcon from '@mui/icons-material/SportsSoccer';
import EventIcon from '@mui/icons-material/Event';
import PeopleIcon from '@mui/icons-material/People';
import NotificationsIcon from '@mui/icons-material/Notifications';
import DataObjectIcon from '@mui/icons-material/DataObject';

const drawerWidth = 240;

const Sidebar = ({ open }) => {
  const location = useLocation();
  
  const menuItems = [
    { 
      text: 'Dashboard', 
      icon: <DashboardIcon />, 
      path: '/' 
    },
    { 
      text: 'Sports', 
      icon: <SportsSoccerIcon />, 
      path: '/sports' 
    },
    { 
      text: 'Matchs', 
      icon: <EventIcon />, 
      path: '/matchs' 
    },
    { 
      text: 'Utilisateurs', 
      icon: <PeopleIcon />, 
      path: '/users' 
    },
    { 
      text: 'Notifications', 
      icon: <NotificationsIcon />, 
      path: '/notifications' 
    },
    { 
      text: 'Pipeline', 
      icon: <DataObjectIcon />, 
      path: '/pipeline' 
    }
  ];

  return (
    <Drawer
      variant="persistent"
      open={open}
      sx={{
        width: drawerWidth,
        flexShrink: 0,
        [`& .MuiDrawer-paper`]: { 
          width: drawerWidth, 
          boxSizing: 'border-box' 
        },
      }}
    >
      <Toolbar />
      <Divider />
      <List component="nav">
        {menuItems.map((item) => (
          <ListItem key={item.text} disablePadding>
            <ListItemButton
              component={Link}
              to={item.path}
              selected={location.pathname === item.path}
            >
              <ListItemIcon>{item.icon}</ListItemIcon>
              <ListItemText primary={item.text} />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
      <Divider />
    </Drawer>
  );
};

export default Sidebar; 