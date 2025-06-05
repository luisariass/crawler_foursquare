from core.user_reviews import UserReviewsExtractor
from config.settings import Settings
import argparse

def main():
    parser = argparse.ArgumentParser(description='Extractor de reseñas de usuarios de Foursquare')
    parser.add_argument('--csv', type=str, default=Settings.USERS_CSV,
                        help='Ruta al archivo CSV con información de usuarios')
    parser.add_argument('--max-workers', type=int, default=2,
                        help='Número de procesos paralelos')
    args = parser.parse_args()

    extractor = UserReviewsExtractor()
    extractor.extract_reviews_from_csv(args.csv, max_workers=args.max_workers)

if __name__ == "__main__":
    main()