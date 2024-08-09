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
    Station_ID CHAR(20) PRIMARY KEY,
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

DELIMITER //
-- create sql trigger for station uuid insertion --
CREATE TRIGGER before_insert_station
BEFORE INSERT ON Station
FOR EACH ROW
BEGIN
    DECLARE new_id CHAR(20);
    DECLARE replica_count INT DEFAULT 0;

-- loop through uuid creation to ensure a unique id at 20 chars--
    REPEAT 
        SET new_id = LEFT(REPLACE(UUID(), '-', ''), 20);

        SELECT COUNT(*) INTO replica_count
        FROM Station
        WHERE Station_ID = new_id;

    UNTIL replica_count = 0
    END REPEAT;
   -- Assign the station id pk value --
    SET NEW.Station_ID = new_id;
END//
DELIMITER ;
