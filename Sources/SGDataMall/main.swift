import Foundation
import Alamofire
import SwiftyJSON

struct BusStop {
    let stopCode: String
    let roadName: String
    let description: String
    let latitude: Double
    let longitude: Double
    
    init(stopCode: String, roadName: String, description: String, latitude: Double, longitude: Double) {
        self.stopCode = stopCode
        self.roadName = roadName
        self.description = description
        self.latitude = latitude
        self.longitude = longitude
    }
    
    static func parse(json: JSON) -> BusStop {
        return BusStop(
            stopCode: json["BusStopCode"].string ?? "",
            roadName: json["RoadName"].string ?? "",
            description: json["Description"].string ?? "",
            latitude: json["Latitude"].double ?? 0.0,
            longitude: json["Longitude"].double ?? 0.0
        )
    }
}

class DataMallClient {
    
    private let apiKey: String
    
    init(apiKey: String) {
        self.apiKey = apiKey
    }
    
    func getBusStopList(completion: @escaping (_ result: [BusStop]) -> Void) {
        let baseUrl = "http://datamall2.mytransport.sg/ltaodataservice/BusStops"
        get(baseUrl: baseUrl, step: 0, result: [], completion: completion)
    }
    
    private func get(baseUrl: String, step: Int, result: [BusStop], completion: @escaping (_ result: [BusStop]) -> Void) {
        let headers: [String: String] = [
            "AccountKey": apiKey
        ]
        let url = "\(baseUrl)?$skip=\(step)"
        print(url)
        
        Alamofire.request(url, method: .get, parameters: nil, encoding: JSONEncoding.default, headers: headers)
            .validate(statusCode: 200..<300).responseJSON { response in
                
                switch response.result {
                case .success:
                    if let json = response.result.value {
                        let data = JSON(json)
                        let list = data["value"].arrayValue.compactMap({ (item) -> BusStop in
                            BusStop.parse(json: item)
                        })
                        
                        if list.isEmpty {
                            completion(result)
                        } else {
                            var ok = [BusStop]()
                            ok.append(contentsOf: result)
                            ok.append(contentsOf: list)
                            self.get(baseUrl: baseUrl, step: step + 500, result: ok, completion: completion)
                        }
                    }
                    
                case .failure(let error):
                    print(error)
                    completion([])
                }
        }
    }
}


let dispatchGroup = DispatchGroup()
dispatchGroup.enter()

DataMallClient(apiKey: "???").getBusStopList { (result) in
    
    let fileURL = URL(fileURLWithPath: "/Users/nguyen/Desktop/sgbustracker.sql")
    try! "".write(to: fileURL, atomically: true, encoding: String.Encoding.utf8)
    
    do {
        let handle = try FileHandle(forWritingTo: fileURL)
        handle.seekToEndOfFile()
        result.forEach({ (it) in
            
            let roadName = it.roadName.replacingOccurrences(of: "\'", with: "\'\'")
            let description = it.description.replacingOccurrences(of: "\'", with: "\'\'")
            let coslat = cos(it.latitude)
            let sinlat = sin(it.latitude)
            let coslng = cos(it.longitude)
            let sinlng = sin(it.longitude)
            
            let sql = "INSERT INTO BusStop(id,road,description,lat,lng,coslat,sinlat,coslng,sinlng) VALUES (\(it.stopCode),'\(roadName)','\(description)',\(it.latitude),\(it.longitude),\(coslat),\(sinlat),\(coslng),\(sinlng));\n"
            
            handle.write(sql.data(using: .utf8)!)
        })
        handle.closeFile()
    } catch {
        print("Error writing to file \(error)")
    }
    
    dispatchGroup.leave()
}

dispatchGroup.notify(queue: DispatchQueue.main) {
    exit(EXIT_SUCCESS)
}
dispatchMain()
