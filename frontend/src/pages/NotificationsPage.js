import React, { useState, useEffect } from 'react';
import {
  Typography,
  Box,
  List,
  ListItem,
  ListItemText,
  ListItemAvatar,
  Avatar,
  Paper,
  Divider,
  Chip,
  Badge,
  FormControl,
  InputLabel,
  Select,
  MenuItem
} from '@mui/material';
import NotificationsIcon from '@mui/icons-material/Notifications';
import SportsIcon from '@mui/icons-material/Sports';
import EventIcon from '@mui/icons-material/Event';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import SportsSoccerIcon from '@mui/icons-material/SportsSoccer';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';

// Importer le service API
import { getNotifications } from '../services/api';

const NotificationsPage = () => {
  const [loading, setLoading] = useState(true);
  const [notifications, setNotifications] = useState([]);
  const [typeFilter, setTypeFilter] = useState('');

  useEffect(() => {
    const fetchNotifications = async () => {
      try {
        // Récupérer les données depuis l'API
        const data = await getNotifications();
        setNotifications(data);
        setLoading(false);
      } catch (error) {
        console.error("Erreur lors du chargement des notifications:", error);
        setLoading(false);
      }
    };

    fetchNotifications();
  }, []);

  // Filtrer les notifications par type
  const filteredNotifications = typeFilter
    ? notifications.filter(notif => notif.type === typeFilter)
    : notifications;

  // Extraire les types uniques pour le filtre
  const uniqueTypes = [...new Set(notifications.map(notif => notif.type))];

  const getNotificationIcon = (type, sportId) => {
    switch (type) {
      case 'debut_match':
        return <PlayArrowIcon />;
      case 'but':
        return <SportsSoccerIcon />;
      case 'fin_match':
        return <CheckCircleIcon />;
      case 'programmé':
        return <EventIcon />;
      default:
        return <SportsIcon />;
    }
  };

  const getNotificationColor = (type) => {
    switch (type) {
      case 'debut_match':
        return '#1976d2'; // blue
      case 'but':
        return '#ff9800'; // orange
      case 'fin_match':
        return '#4caf50'; // green
      case 'programmé':
        return '#9c27b0'; // purple
      case 'carton':
        return '#f44336'; // red
      default:
        return '#757575'; // grey
    }
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" gutterBottom>
          Notifications
        </Typography>
        
        <Badge badgeContent={notifications.filter(n => n.statut === 'envoyée').length} color="error">
          <NotificationsIcon />
        </Badge>
      </Box>
      
      {/* Filtre par type */}
      <Box mb={3}>
        <FormControl sx={{ minWidth: 200 }}>
          <InputLabel>Filtrer par type</InputLabel>
          <Select
            value={typeFilter}
            label="Filtrer par type"
            onChange={(e) => setTypeFilter(e.target.value)}
          >
            <MenuItem value="">Tous les types</MenuItem>
            {uniqueTypes.map(type => (
              <MenuItem key={type} value={type}>{type}</MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>
      
      {loading ? (
        <Typography>Chargement des notifications...</Typography>
      ) : (
        <Paper sx={{ maxHeight: 'calc(100vh - 250px)', overflow: 'auto' }}>
          <List>
            {filteredNotifications.map((notification, index) => (
              <React.Fragment key={notification.notification_id}>
                <ListItem alignItems="flex-start">
                  <ListItemAvatar>
                    <Avatar sx={{ bgcolor: getNotificationColor(notification.type) }}>
                      {getNotificationIcon(notification.type, notification.sport_id)}
                    </Avatar>
                  </ListItemAvatar>
                  <ListItemText
                    primary={
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Typography variant="subtitle1">
                          {notification.contenu}
                        </Typography>
                        {notification.sport_nom && (
                          <Chip 
                            label={notification.sport_nom} 
                            size="small" 
                            color="primary" 
                            variant="outlined"
                            sx={{ ml: 1 }}
                          />
                        )}
                        {notification.statut === 'envoyée' && (
                          <Chip
                            label="Nouvelle"
                            size="small"
                            color="error"
                            sx={{ ml: 1 }}
                          />
                        )}
                      </Box>
                    }
                    secondary={
                      <Typography
                        sx={{ display: 'inline' }}
                        component="span"
                        variant="body2"
                        color="text.secondary"
                      >
                        {new Date(notification.date_envoi).toLocaleString()}
                      </Typography>
                    }
                  />
                </ListItem>
                {index < filteredNotifications.length - 1 && <Divider variant="inset" component="li" />}
              </React.Fragment>
            ))}
          </List>
        </Paper>
      )}
    </Box>
  );
};

export default NotificationsPage; 