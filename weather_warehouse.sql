USE WeatherData;

CREATE TABLE Station (
    Station_ID CHAR(4) PRIMARY KEY,
    Latitude FLOAT(10),
    Longitude FLOAT(10),
    City VARCHAR(15),
    State CHAR(2),
    Country CHAR(2),
    Weather_ID INT,
    Pollutant_ID INT,
    FOREIGN KEY (Weather_ID) REFERENCES Weather(Weather_ID),
    FOREIGN KEY (Pollutant_ID) REFERENCES Pollutants(Pollutant_ID)
);

CREATE TABLE Weather (
    Weather_ID INT AUTO_INCREMENT PRIMARY KEY,
    Temperature_F FLOAT(5, 2),
    Weather VARCHAR(20),
    Humidity INT(3),
    Date DATE,
    Time TIME
);

CREATE TABLE Pollutants (
    Pollutant_ID INT AUTO_INCREMENT PRIMARY KEY,
    Carbon_Monoxide FLOAT(7, 2),
    Nitrogen_Dioxide FLOAT(6, 2),
    Ozone FLOAT(6, 2),
    Sulfur_Dioxide FLOAT(5, 2),
    Particulate_Matter FLOAT(5, 2),
    Ammonia FLOAT(5, 2)
);