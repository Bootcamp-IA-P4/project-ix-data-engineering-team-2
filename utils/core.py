class Settings:
    def __init__(self):
        self.proyect_name = "DataTech Solutions"
        self.description = "Implementar un sistema de gestión de datos eficiente para HR Pro, que les permita organizar y analizar grandes volúmenes de datos procedentes de diversas fuentes, como solicitudes de empleo, registros de nómina, encuestas de empleados, entre otros."
        self.slogan = "Equipo de ingenieros de datos freelance"
        self.bootstrap_port = 29092
        self.kafka_host = 'localhost' #100.64.200.34
        self.topic = 'probando'
        self.group_id = 'test_group'
        self.version = "1.0.0"
        self.api_prefix = "/api"
        self.api_version = "/v1"
        self.creators = ["Juan", "Polina", "Orlando", "Juan Carlos"]

settings = Settings()