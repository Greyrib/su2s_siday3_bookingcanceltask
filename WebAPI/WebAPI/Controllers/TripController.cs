using Microsoft.AspNetCore.Mvc;

namespace WebAPI.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TripController : ControllerBase
{
    [HttpGet("/book")]
    public async IActionResult Book(string name, string email, string trip, string type)
    {


        // Send to messaging

    }
}
