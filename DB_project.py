from sqlalchemy.orm import sessionmaker
import sqlalchemy as db
from sqlalchemy import Column, String, Integer, ForeignKey, func
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text
#
#Drivers: ID, first_name, last_name, car, region
#Cars: ID, brand, model, colour
#Regions: ID, name
#
#*****************************************TABLES******************************

Base = declarative_base()

class Regions(Base):
    __tablename__ = "Regions"
    ID = Column("ID", Integer, primary_key = True, autoincrement=True)
    Name = Column("Name", String(24))

    def __init__(self, Name):
        #self.ID=ID
        self.Name=Name
        return

class Cars(Base):
    __tablename__ = "Cars"
    ID = Column("ID", Integer, primary_key = True, autoincrement=True)
    Brand = Column("Brand", String(64))
    Model = Column("Model", String(32))
    Colour = Column("Colour", String(32))

    def __init__(self, Brand, Model, Colour):
        #self.ID=ID
        self.Brand=Brand
        self.Model=Model
        self.Colour=Colour
        return


class Drivers(Base):
    __tablename__ = "Drivers"
    ID = Column("ID", Integer, primary_key = True, unique=True, nullable=False, autoincrement=True)
    FirstName = Column("First Name", String(24))
    LastName = Column("Last Name", String(64))
    Car = Column("Car", Integer, ForeignKey(Cars.ID), unique=True, nullable=False)
    Region = Column("Region", Integer, ForeignKey(Regions.ID), nullable=False)

    def __init__(self, FS, LS, Car, Reg):
        #self.ID=ID
        self.FirstName=FS
        self.LastName=LS
        self.Car=Car
        self.Region=Reg

        return


#*****************************************************Operations**************************************** 
    
def regions():
    session.add(Regions("ABC"))
    session.add(Regions("DEF"))
    session.add(Regions("GHI"))
    session.commit()

def create_all():
    Base.metadata.create_all(engine)
    return


#****************************Drivers Full CRUD
def new_driver(FS, LS, Car, Reg):
    session.add(Drivers( FS, LS, Car, Reg))
    try:
        session.commit()
    except IntegrityError:
        print("Invalid_data")
        session.rollback()
    return

def show_drivers():
    stm = session.query(Drivers).all()
    for row in stm:
        print(row.FirstName, row.LastName)
    return

def update_driver(ID):
    target=session.query(Drivers).filter(Drivers.ID == ID)

    print("Choose column:\n1.First name\n2.Last name\n3.Car\n4.Region\n")
    print("To return to main menu press any other key.")

    chosen = int(input(""))
        
    
    match chosen:
        case 1:
            new_data=input("Input:")
            target.update({Drivers.FirstName: new_data})
        case 2:
            new_data=input("Input:")
            target.update({Drivers.LastName: new_data})
        case 3:
            new_data=input("Input:")
            target.update({Drivers.Car: new_data})
        case 4:
            new_data=input("Input:")
            if new_data < 3 and new_data > 1:
                target.update({Drivers.Region: new_data})
            else:
                print("No such region.\n")
        case _:
            pass
    session.commit()
    return

                
def fire_driver(ID):
    target=session.query(Drivers).filter(Drivers.ID == ID)
    target.delete()
    session.commit()

#************************************************       

def buy_new_car(Brand, Model, Colour):
    session.add(Cars(Brand, Model, Colour))
    session.commit()

def show_cars():
    stm = session.query(Cars).all()
    for row in stm:
        print(row.Model, row.Brand)
    return

def show_regions():
    stm = session.query(Regions).all()
    for row in stm:
        print(row.Name)
    return

def show_drivers_with_cars():
    stm = session.query(Drivers.ID, Drivers.LastName, Cars.ID, Cars.Brand, Cars.Model).join(Cars, Drivers.Car == Cars.ID)
    #print()
    for row in stm:
        print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]}, {row[4]}")
    return

def drivers_regions():
    stm = session.query(Regions.Name, func.count(Drivers.ID)).join(Drivers, Drivers.Region == Regions.ID).group_by(Regions.Name)
    #print()
    for row in stm:
        print(f"{row[0]} | {row[1]}")
    return


#****************************************************Connection************************************

try:
    engine = db.create_engine("mysql+mysqlconnector://root:root@localhost:3333/Projekt", echo=True)
    engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

except Exception:
    print("Database connection denied")
    exit(0)


while True:
    print("\nChoose operation:")
    print("1.Show drivers")
    print("2.Show regions")
    print("3.Show cars")
    print("4.Update driver's data")
    print("5.Add new driver")
    print("6.Fire driver")
    print("7.Buy new car")
    print("8.Show drivers with their cars")
    print("9.Show Regions with number of drivers")
    print("\n To exit press 0")

    a = int(input(""))

    match a:
        case 1:
            show_drivers()
        case 2:
            show_regions()
        case 3:
            show_cars()
        case 4:
            update_driver(int(input("Drivers ID:")))
        case 5:
            Fs = input("First Name: ")
            Ls = input("Last Name: ")
            Car = int(input("Car: "))
            Reg = int(input("Region: "))
            new_driver(Fs,Ls, Car, Reg)
        case 6:
            fire_driver(int(input("Drivers ID:")))
        case 7:
            B=input("Brand: ")
            M=input("Model: ")
            C=input("Colour: ")
            buy_new_car(B,M,C)
        case 8:
            show_drivers_with_cars()
        case 9:
            drivers_regions()
        case 0:
            exit(0)
            


