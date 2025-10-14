"""
Módulo para la configuración y gestión de la base de datos.

Define el esquema de las tablas usando SQLAlchemy y proporciona una función
para inicializar la base de datos.
"""

import os
from sqlalchemy import (
    create_engine, Column, String, Text, Numeric, DateTime, ForeignKey,
    UniqueConstraint
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.sql import func

# --- Configuración de la Conexión ---
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user_db:pass_db@localhost:5432/sities_db"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- Definición de las Tablas (Modelos) ---

class Site(Base):
    """Modelo para la tabla 'sites'."""
    __tablename__ = 'sites'

    id = Column(String(255), primary_key=True)
    name = Column(Text, nullable=False)
    url = Column(Text, unique=True, nullable=False)
    category = Column(Text)
    address = Column(Text)
    rating = Column(Numeric(3, 1))
    municipality = Column(String(255), index=True)

    # Timestamps de control
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    reviewers_scraped_at = Column(DateTime(timezone=True), nullable=True)

    # Relación: Un sitio tiene muchos 'site_reviewer_association'
    reviewer_associations = relationship(
        "SiteReviewerAssociation", back_populates="site"
    )

    def __repr__(self):
        return f"<Site(id='{self.id}', name='{self.name}')>"


class Reviewer(Base):
    """Modelo para la tabla 'reviewers'."""
    __tablename__ = 'reviewers'

    id = Column(String(255), primary_key=True)
    name = Column(Text, nullable=False)
    url = Column(Text, unique=True, nullable=False)

    # Timestamps de control
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    tips_scraped_at = Column(DateTime(timezone=True), nullable=True)

    # Relación: Un reseñante tiene muchos 'site_reviewer_association' y tips
    site_associations = relationship(
        "SiteReviewerAssociation", back_populates="reviewer"
    )
    tips = relationship("Tip", back_populates="reviewer")

    def __repr__(self):
        return f"<Reviewer(id='{self.id}', name='{self.name}')>"


class SiteReviewerAssociation(Base):
    """Tabla de asociación para la relación muchos a muchos."""
    __tablename__ = 'site_reviewer_association'

    site_id = Column(String, ForeignKey('sites.id'), primary_key=True)
    reviewer_id = Column(String, ForeignKey('reviewers.id'), primary_key=True)

    site = relationship("Site", back_populates="reviewer_associations")
    reviewer = relationship("Reviewer", back_populates="site_associations")


class Tip(Base):
    """Modelo para la tabla 'tips'."""
    __tablename__ = 'tips'

    id = Column(String(255), primary_key=True)
    text = Column(Text, nullable=False)
    tip_url = Column(Text, unique=True)
    created_at_foursquare = Column(DateTime(timezone=True))

    # Claves foráneas
    reviewer_id = Column(String, ForeignKey('reviewers.id'), index=True)
    site_id = Column(String, ForeignKey('sites.id'), index=True)

    # Timestamp de control
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relaciones
    reviewer = relationship("Reviewer", back_populates="tips")

    def __repr__(self):
        return f"<Tip(id='{self.id}')>"


def init_db():
    """
    Crea todas las tablas en la base de datos si no existen.
    """
    print("Inicializando la base de datos y creando tablas si es necesario...")
    Base.metadata.create_all(bind=engine)
    print("Tablas creadas exitosamente.")


if __name__ == "__main__":
    init_db()