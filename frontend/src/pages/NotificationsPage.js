import React, { useState, useEffect } from "react";
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
  MenuItem,
  Button,
  Snackbar,
  Alert,
  Grow,
} from "@mui/material";
import NotificationsIcon from "@mui/icons-material/Notifications";
import SportsIcon from "@mui/icons-material/Sports";
import EventIcon from "@mui/icons-material/Event";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import SportsSoccerIcon from "@mui/icons-material/SportsSoccer";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import RefreshIcon from "@mui/icons-material/Refresh";

// Importer le service API
import { getNotifications } from "../services/api";

const NotificationsPage = () => {
  const [loading, setLoading] = useState(true);
  const [notifications, setNotifications] = useState([]);
  const [typeFilter, setTypeFilter] = useState("");
  const [refreshInterval, setRefreshInterval] = useState(null);
  const [newNotificationIds, setNewNotificationIds] = useState([]);
  const [snackbar, setSnackbar] = useState({
    open: false,
    message: "",
    severity: "info",
  });

  // Fonction utilitaire pour parser une date de manière sécurisée
  const parseDate = (dateString) => {
    if (!dateString) return new Date(0); // Date par défaut si null/undefined

    try {
      const date = new Date(dateString);
      // Vérifier si la date est valide
      return isNaN(date.getTime()) ? new Date(0) : date;
    } catch (e) {
      console.error("Erreur lors du parsing de date:", dateString, e);
      return new Date(0);
    }
  };

  const fetchNotifications = async () => {
    try {
      // Récupérer les données depuis l'API
      const data = await getNotifications();

      // Stocker les IDs des notifications existantes
      const existingIds = notifications.map((n) => n.notification_id);

      // Trier les notifications par date, de la plus récente à la plus ancienne
      const sortedData = [...data].sort(
        (a, b) => parseDate(b.date_envoi) - parseDate(a.date_envoi)
      );

      // Identifier les nouvelles notifications
      const newIds = sortedData
        .filter((n) => !existingIds.includes(n.notification_id))
        .map((n) => n.notification_id);

      if (newIds.length > 0 && notifications.length > 0) {
        console.log("Nouvelles notifications:", newIds);
        setSnackbar({
          open: true,
          message: `${newIds.length} nouvelle(s) notification(s) reçue(s)!`,
          severity: "success",
        });

        // Mettre à jour les IDs des nouvelles notifications pour l'animation
        setNewNotificationIds(newIds);

        // Effacer les IDs après quelques secondes pour arrêter l'animation
        setTimeout(() => {
          setNewNotificationIds([]);
        }, 5000);
      }

      setNotifications(sortedData);
      setLoading(false);
    } catch (error) {
      console.error("Erreur lors du chargement des notifications:", error);
      setLoading(false);
      setSnackbar({
        open: true,
        message: "Erreur lors du chargement des notifications",
        severity: "error",
      });
    }
  };

  useEffect(() => {
    // Charger les notifications au démarrage
    fetchNotifications();

    // Mettre en place un rafraîchissement automatique toutes les 30 secondes
    const interval = setInterval(() => {
      fetchNotifications();
    }, 30000);

    setRefreshInterval(interval);

    // Nettoyer l'intervalle à la destruction du composant
    return () => {
      if (refreshInterval) clearInterval(refreshInterval);
    };
  }, []);

  // Fermer la snackbar
  const handleCloseSnackbar = () => {
    setSnackbar({ ...snackbar, open: false });
  };

  // Rafraîchir manuellement les notifications
  const handleRefresh = () => {
    setLoading(true);
    fetchNotifications();
  };

  // Formater la date de manière plus lisible
  const formatDate = (dateString) => {
    try {
      const date = new Date(dateString);
      if (isNaN(date.getTime())) return "Date invalide";

      // Formater la date en français
      return date.toLocaleDateString("fr-FR", {
        day: "numeric",
        month: "short",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
    } catch (e) {
      return "Date invalide";
    }
  };

  // Filtrer les notifications par type
  const filteredNotifications = typeFilter
    ? notifications.filter((notif) => notif.type === typeFilter)
    : notifications;

  // Extraire les types uniques pour le filtre
  const uniqueTypes = [...new Set(notifications.map((notif) => notif.type))];

  const getNotificationIcon = (type, sportId) => {
    switch (type) {
      case "debut_match":
        return <PlayArrowIcon />;
      case "but":
        return <SportsSoccerIcon />;
      case "fin_match":
        return <CheckCircleIcon />;
      case "programmé":
        return <EventIcon />;
      default:
        return <SportsIcon />;
    }
  };

  const getNotificationColor = (type) => {
    switch (type) {
      case "debut_match":
        return "#1976d2"; // blue
      case "but":
        return "#ff9800"; // orange
      case "fin_match":
        return "#4caf50"; // green
      case "programmé":
        return "#9c27b0"; // purple
      case "carton":
        return "#f44336"; // red
      default:
        return "#757575"; // grey
    }
  };

  return (
    <Box>
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 3,
        }}
      >
        <Typography variant="h4" gutterBottom>
          Notifications
        </Typography>

        <Box display="flex" alignItems="center" gap={2}>
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            onClick={handleRefresh}
            disabled={loading}
          >
            Rafraîchir
          </Button>
          <Badge
            badgeContent={
              notifications.filter((n) => n.statut === "envoyée").length
            }
            color="error"
          >
            <NotificationsIcon />
          </Badge>
        </Box>
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
            {uniqueTypes.map((type) => (
              <MenuItem key={type} value={type}>
                {type}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>

      {loading ? (
        <Typography>Chargement des notifications...</Typography>
      ) : (
        <Paper sx={{ maxHeight: "calc(100vh - 250px)", overflow: "auto" }}>
          {filteredNotifications.length === 0 ? (
            <Box p={3} textAlign="center">
              <Typography variant="body1">
                Aucune notification à afficher.
              </Typography>
            </Box>
          ) : (
            <List>
              {filteredNotifications.map((notification, index) => (
                <React.Fragment key={notification.notification_id}>
                  <Grow
                    in={true}
                    style={{
                      transformOrigin: "0 0 0",
                      transitionDuration: newNotificationIds.includes(
                        notification.notification_id
                      )
                        ? "1000ms"
                        : "0ms",
                    }}
                    timeout={
                      newNotificationIds.includes(notification.notification_id)
                        ? 1000
                        : 0
                    }
                  >
                    <ListItem
                      alignItems="flex-start"
                      sx={{
                        backgroundColor: newNotificationIds.includes(
                          notification.notification_id
                        )
                          ? "rgba(76, 175, 80, 0.1)" // Légère teinte verte pour les nouvelles notifications
                          : "transparent",
                        transition: "background-color 2s ease",
                      }}
                    >
                      <ListItemAvatar>
                        <Avatar
                          sx={{
                            bgcolor: getNotificationColor(notification.type),
                          }}
                        >
                          {getNotificationIcon(
                            notification.type,
                            notification.sport_id
                          )}
                        </Avatar>
                      </ListItemAvatar>
                      <ListItemText
                        primary={
                          <Box
                            sx={{
                              display: "flex",
                              alignItems: "center",
                              gap: 1,
                            }}
                          >
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
                            {(notification.statut === "envoyée" ||
                              newNotificationIds.includes(
                                notification.notification_id
                              )) && (
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
                            sx={{ display: "inline" }}
                            component="span"
                            variant="body2"
                            color="text.secondary"
                          >
                            {formatDate(notification.date_envoi)}
                          </Typography>
                        }
                      />
                    </ListItem>
                  </Grow>
                  {index < filteredNotifications.length - 1 && (
                    <Divider variant="inset" component="li" />
                  )}
                </React.Fragment>
              ))}
            </List>
          )}
        </Paper>
      )}

      {/* Snackbar for notifications */}
      <Snackbar
        open={snackbar.open}
        autoHideDuration={6000}
        onClose={handleCloseSnackbar}
        anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
      >
        <Alert
          onClose={handleCloseSnackbar}
          severity={snackbar.severity}
          sx={{ width: "100%" }}
        >
          {snackbar.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default NotificationsPage;
