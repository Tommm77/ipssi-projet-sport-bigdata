import React, { useState, useEffect } from 'react';
import { Typography, Grid, Paper, Box, Card, CardContent, Chip } from '@mui/material';
import SportsSoccerIcon from '@mui/icons-material/SportsSoccer';
import EventIcon from '@mui/icons-material/Event';
import PeopleIcon from '@mui/icons-material/People';
import NotificationsIcon from '@mui/icons-material/Notifications';

// Importer les services API
import { getSportsStats, getMatchsStats, getServicesHealth } from '../services/api';

const Dashboard = () => {
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState({
    sports: 0,
    matchs: {
      total: 0,
      programmés: 0,
      enCours: 0,
      terminés: 0
    },
    users: 0,
    notifications: 0
  });
  const [servicesHealth, setServicesHealth] = useState({});

  useEffect(() => {
    // Charger les vraies données depuis l'API
    const fetchData = async () => {
      try {
        // Récupérer les statistiques depuis l'API
        const sportsStatsData = await getSportsStats();
        const matchsStatsData = await getMatchsStats();
        const healthData = await getServicesHealth();
        
        // Compter le nombre de sports
        const sportsCount = sportsStatsData.length;
        
        // Extraire les stats des matchs
        const matchsTotal = matchsStatsData.par_statut.programmé +
                           matchsStatsData.par_statut.en_cours +
                           matchsStatsData.par_statut.terminé;
        
        // Mettre à jour l'état
        setStats({
          sports: sportsCount,
          matchs: {
            total: matchsTotal,
            programmés: matchsStatsData.par_statut.programmé || 0,
            enCours: matchsStatsData.par_statut.en_cours || 0,
            terminés: matchsStatsData.par_statut.terminé || 0
          },
          users: 250, // À remplacer par une vraie API quand disponible
          notifications: 128 // À remplacer par une vraie API quand disponible
        });
        
        setServicesHealth(healthData);
        setLoading(false);
      } catch (error) {
        console.error("Erreur lors du chargement des données:", error);
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const statCards = [
    {
      title: "Sports",
      value: stats.sports,
      icon: <SportsSoccerIcon fontSize="large" color="primary" />,
      color: "#1976d2"
    },
    {
      title: "Matchs",
      value: stats.matchs.total,
      icon: <EventIcon fontSize="large" color="secondary" />,
      color: "#dc004e",
      details: [
        { label: "Programmés", value: stats.matchs.programmés, color: "primary" },
        { label: "En cours", value: stats.matchs.enCours, color: "success" },
        { label: "Terminés", value: stats.matchs.terminés, color: "default" }
      ]
    },
    {
      title: "Utilisateurs",
      value: stats.users,
      icon: <PeopleIcon fontSize="large" style={{ color: "#ff9800" }} />,
      color: "#ff9800"
    },
    {
      title: "Notifications",
      value: stats.notifications,
      icon: <NotificationsIcon fontSize="large" style={{ color: "#4caf50" }} />,
      color: "#4caf50"
    }
  ];

  return (
    <Box className="dashboard">
      <Typography variant="h4" gutterBottom>
        Tableau de bord
      </Typography>
      
      {/* Statut des services */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Statut des services
        </Typography>
        <Grid container spacing={2}>
          {Object.entries(servicesHealth).map(([service, status]) => (
            <Grid item key={service}>
              <Chip
                label={`${service}: ${status}`}
                color={status === 'ok' ? 'success' : 'error'}
                variant="outlined"
              />
            </Grid>
          ))}
        </Grid>
      </Paper>
      
      <Grid container spacing={3}>
        {statCards.map((card, index) => (
          <Grid item xs={12} sm={6} md={3} key={index}>
            <Card>
              <CardContent>
                <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
                  <Typography variant="h6" color="textSecondary">
                    {card.title}
                  </Typography>
                  {card.icon}
                </Box>
                <Typography variant="h3" component="div" style={{ color: card.color }}>
                  {card.value}
                </Typography>
                
                {card.details && (
                  <Box display="flex" flexWrap="wrap" gap={1} mt={2}>
                    {card.details.map((detail, idx) => (
                      <Chip 
                        key={idx} 
                        label={`${detail.label}: ${detail.value}`} 
                        color={detail.color} 
                        size="small" 
                        variant="outlined" 
                      />
                    ))}
                  </Box>
                )}
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>
      
      <Box mt={4}>
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Sports les plus populaires
              </Typography>
              {/* Ici on pourrait ajouter un graphique avec Chart.js */}
              <Typography variant="body1">
                1. Football (42 matchs)
              </Typography>
              <Typography variant="body1">
                2. Basketball (38 matchs)
              </Typography>
              <Typography variant="body1">
                3. Tennis (15 matchs)
              </Typography>
            </Paper>
          </Grid>
          
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Activité récente
              </Typography>
              <Typography variant="body2" color="textSecondary">
                • Match PSG vs Marseille terminé: 2-1
              </Typography>
              <Typography variant="body2" color="textSecondary">
                • Nouveau match programmé: ASVEL vs Monaco (Basketball)
              </Typography>
              <Typography variant="body2" color="textSecondary">
                • 15 nouvelles notifications envoyées
              </Typography>
            </Paper>
          </Grid>
        </Grid>
      </Box>
    </Box>
  );
};

export default Dashboard; 