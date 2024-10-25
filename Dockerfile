# Use Golang image based on Debian Bookworm
FROM golang:bookworm

# Set the working directory within the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application source code
COPY ./*.go .
COPY ./templates ./templates

# Build the Go application
RUN go build -o relay .

# Create a non-privileged user
RUN groupadd -g 900 relay && useradd -u 900 -g 900 -m relay

# Change ownership of the application directory to the non-privileged user
RUN chown -R relay:relay /app

# Switch to the non-privileged user
USER relay

# Expose the port that the application will run on
EXPOSE 3336

# Set the command to run the executable
CMD ["./relay"]
