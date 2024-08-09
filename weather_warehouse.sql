USE WeatherData;

CREATE TABLE Weather (
    Weather_ID INT AUTO_INCREMENT PRIMARY KEY,
    Temperature_F FLOAT,
    Weather VARCHAR(20),
    Humidity INT(3),
    Date DATE,
    Time_EST TIME
);

CREATE TABLE Pollutants (
    Pollutant_ID INT AUTO_INCREMENT PRIMARY KEY,
    Carbon_Monoxide FLOAT,
    Nitrogen_Dioxide FLOAT,
    Ozone FLOAT,
    Sulfur_Dioxide FLOAT,
    Particulate_Matter FLOAT,
    Ammonia FLOAT
);

CREATE TABLE Station (
    Station_ID CHAR(4) PRIMARY KEY,
    Latitude FLOAT,
    Longitude FLOAT,
    City VARCHAR(15),
    State CHAR(2),
    Country CHAR(2),
    Weather_ID INT,
    Pollutant_ID INT,
    FOREIGN KEY (Weather_ID) REFERENCES Weather(Weather_ID),
    FOREIGN KEY (Pollutant_ID) REFERENCES Pollutants(Pollutant_ID)
);