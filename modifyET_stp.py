#!/usr/bin/env python
# coding: utf-8
import csv, argparse, sys, re, os
        
append_text = ",  ADULTENTERTAINMENT, AGRICULTURE, AI, AUTOVEHICLE, BABYBOOMERS, BIGDATA, BITCOIN, BLOCKCHAIN, BTCCASH, BUYBACK, CANCERTREATMENT, CARS, CATHOLIC, CHILDREN, CLEANENERGY, CLEANTECH, CLIMATECHANGE, CLOUD, COAL, COINS, COMPUTERVISION, CYBERSECURITY, DIGITALHEALTH, DRIPIRRIGATION, DRONE, ECOMMERCE, ELECTRICVEHICLE, EMPLOYEETREATMENT, ENTREPRENEURSHIP, ENVIRONMENTALPOS, ESGPOS, ETHEREUM, FINTECH, FOOD, FORESTRY, FOSSILFREE, GAMING, GENDER, GENX, GEOTHERMAL, HEALTHTECH, HEALTHCARE, HOMEOWNERSHIP, HUMANRIGHTS, IMMUNOTHERAPY, INFRASTRUCTURE, INSURTECH, IOT, IPOS, LEISURE, LENDINGPLATFORMS, LGBT, LOGISTICS, LUXURY, MARIJUANA, MEDICALDEVICES, MEDICALTECH, MILITARY, MILITARYSPENDING, MILLENIALS, MRI, MUSIC, NANOTECH, NLP, NUCLEAR, ORGANICFOOD, OUTSOURCING, PAYMENTS, PETS, PHILANTHROPY, PRECISIONAG, PRECISIONMED, QSR, QUANTUM, RENEWABLEENERGY, RIPPLE, ROBOADVISOR, ROBOTICS, SAAS, SENIORS, SENSORS, SHALE, SMARTPHONES, SOCIALMEDIA, SOCIALPOS, SOLAR, SPINOFFS, SPORTS, STEMCELLS, STREAMING, TECHNOLOGY, TELECOM, THREEDPRINTING, TOYS, TRAVEL, VEGAN, VICE, VIRTUALREALITY, WASTE, WATER, WEALTHTECH, WEARABLETECH, WELLNESS, WIND, WOMEN"

def add_entities(filename=""):
    # print(filename)
    f = open(filename,"r")
    s = f.readlines()
    s2 = []
    for i in s:
        if i == '\n':
            continue
        s2.append(i[:-1] + append_text)
        s2.append('\n')
        s2.append('\n')
        # print(i)
    # print(s[:2])
    f.close()
    f2 = open(filename, "w+")
    f2.writelines(s2)
    f2.close()
    #print(s2[:2])


# filename = r"C:\Users\Opensesafi\Documents\VIPProjects\STK_6-13\eventtypes2.txt"

# add_entities(filename)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Modifies an eventtypes.txt based on all events in results file to add Entities as in this script.')
    parser.add_argument('--document', type=str, help='the results file.', required=True)
    # parser.add_argument('--tag', type=str, help='what to add to the end of the eventtypes.txt file name.', required=True)
    args = parser.parse_args()
    add_entities(args.document)
    print("Done!")
    
    # (directory, x) = os.path.split(args.document)
    # _main(args.document, directory, args.tag)



