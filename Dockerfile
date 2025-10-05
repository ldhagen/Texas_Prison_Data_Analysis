FROM php:8.2-apache

# Install system dependencies and build tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    gcc \
    g++ \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    libzip-dev \
    zip \
    unzip \
    git \
    && docker-php-ext-install zip pdo pdo_mysql \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip and install wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install numpy first (pandas dependency)
RUN pip install --no-cache-dir numpy

# Install pandas and pyarrow with longer timeout
RUN pip install --no-cache-dir --default-timeout=1000 pandas pyarrow

# Enable Apache mod_rewrite
RUN a2enmod rewrite

# Set working directory
WORKDIR /var/www/html

# Create necessary directories with proper permissions
RUN mkdir -p /var/www/html/cache /var/www/html/exports /var/www/html/data && \
    chown -R www-data:www-data /var/www/html && \
    chmod -R 777 /var/www/html/cache /var/www/html/exports

# Expose port
EXPOSE 80

CMD ["apache2-foreground"]