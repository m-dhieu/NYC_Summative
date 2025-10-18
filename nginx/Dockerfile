FROM nginx:1.25-alpine

# Copy built static site (we serve repo's index.html + frontend)
COPY ./index.html /usr/share/nginx/html/index.html
COPY ./frontend /usr/share/nginx/html/frontend

# Copy nginx config
COPY ./nginx/nginx.conf /etc/nginx/nginx.conf

EXPOSE 80
