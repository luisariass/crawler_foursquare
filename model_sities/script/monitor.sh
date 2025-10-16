#!/bin/bash
# filepath: c:\Users\luisarias\Documents\proyecto_scrapping\model_sites\script\monitor.sh

CONTAINER_SCRAPER="foursquare_sities_prod"

check_containers() {
    echo "[INFO] Estado del contenedor:"
    docker ps --filter "name=foursquare_sities_prod" --format "table {{.Names}}\t{{.Status}}"
}

check_scraper_health() {
    echo "[INFO] Verificando salud del scraper..."
    HEALTH=$(docker inspect --format='{{.State.Health.Status}}' ${CONTAINER_SCRAPER} 2>/dev/null)
    echo "[INFO] Estado de salud: ${HEALTH}"
}

show_logs() {
    echo "[INFO] Últimas 50 líneas de logs:"
    docker logs --tail=50 ${CONTAINER_SCRAPER}
}

show_stats() {
    echo "[INFO] Estadísticas de recursos:"
    docker stats --no-stream ${CONTAINER_SCRAPER}
}

restart_container() {
    echo "[INFO] Reiniciando contenedor..."
    docker-compose -f docker-compose.prod.yml restart
    sleep 10
    check_containers
}

case "${1}" in
    status)
        check_containers
        check_scraper_health
        ;;
    logs)
        show_logs
        ;;
    stats)
        show_stats
        ;;
    restart)
        restart_container
        ;;
    full)
        check_containers
        check_scraper_health
        show_logs
        ;;
    *)
        echo "Uso: $0 {status|logs|stats|restart|full}"
        exit 1
        ;;
esac